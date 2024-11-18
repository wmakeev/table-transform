import { TransformStepError } from '../errors/index.js'
import {
  TableChunksSource,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  splitChunksBy,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { getNormalizedHeaderRow } from '../tools/header/index.js'

export interface SplitInParams {
  /**
   * The columns by which the incoming stream is splited
   */
  keyColumns: string[]

  /**
   * The config of transformation through which each of the splited parts of
   * the stream is passed.
   */
  transformConfig: TableTransfromConfig
}

const TRANSFORM_NAME = 'SplitIn'

async function sourceSplitByToChannels(
  source: TableChunksSource,
  keyColumns: string[],
  targetChannelsChan: AsyncChannel<AsyncChannel<TableRow[]>>
): Promise<void> {
  let chan: AsyncChannel<TableRow[]> | null = null

  for await (const [isGroupFistChunk, chunk] of splitChunksBy(
    source,
    keyColumns
  )) {
    if (isGroupFistChunk) {
      if (chan != null) {
        if (!chan.isClosed()) {
          if (!chan.isFlushed()) await chan.flush()
          chan.close()
        }
      }

      chan = new AsyncChannel<TableRow[]>({
        name: `${TRANSFORM_NAME}:chan`
      })

      await targetChannelsChan.put(chan)
    }

    if (chan == null) continue

    if (chan.isClosed()) {
      chan = null
      continue
    }

    chan.put(chunk)
  }

  if (chan?.isFlushed() === false) await chan?.flush()
  chan?.close()

  if (!targetChannelsChan.isFlushed()) await targetChannelsChan.flush()
  targetChannelsChan.close()
}

/**
 * SplitIn
 */
export const splitIn = (params: SplitInParams): TableChunksTransformer => {
  const { keyColumns } = params

  return source => {
    const srcHeader = source.getHeader()

    // Drop headers from splits
    const transformConfig: TableTransfromConfig = {
      ...params.transformConfig,
      outputHeader: {
        ...params.transformConfig.outputHeader,
        skip: true
      }
    }

    //#region #jsgf360l

    // TODO Этот блок копипаста из createTableTransformer.
    // Явно есть просчеты в архитектуре, раз приходится городить такие костыли.
    // Фактически сейчас без подобных костылей не получить заголовки по конфигурации.
    // Возможно это нормально? Ведь в этом модуле приходится несколько раз вызывать
    // трансформацию с одинаковым конфигом (все ли созданные трансформации можно
    // вызывать повторно?), но выглядить это запутанно и очень сложно отлаживать.

    const transforms_ = [...(transformConfig.transforms ?? [])]

    // Ensure all forsed columns exist and select
    if (transformConfig.outputHeader?.forceColumns != null) {
      transforms_.push(
        tf.column.select({
          columns: transformConfig.outputHeader.forceColumns,
          addMissingColumns: true
        })
      )
    }

    transforms_.push(tf.normalize({ immutable: false }))

    let tableSource = source

    // Chain transformations
    for (const transform of transforms_) {
      tableSource = transform(tableSource)
    }

    const transfomedHeader = tableSource.getHeader()
    //#endregion

    async function* getTransformedSourceGenerator() {
      const normalizedHeaderColumns = getNormalizedHeaderRow(srcHeader)

      // TODO Выделить в отдельный хелпер?
      //#region Check columns in source
      const headerColumnsSet = new Set(normalizedHeaderColumns)

      const notFoundColumns = []

      for (const col of keyColumns) {
        if (!headerColumnsSet.has(col)) notFoundColumns.push(col)
      }

      if (notFoundColumns.length !== 0) {
        throw new TransformStepError(
          `Columns not found - ${notFoundColumns.map(col => `"${col}"`).join(', ')}`,
          TRANSFORM_NAME
        )
      }
      //#endregion

      const channels = new AsyncChannel<AsyncChannel<TableRow[]>>({
        name: `${TRANSFORM_NAME}:channels`
      })

      const pipeSourceToChannelsPromise = sourceSplitByToChannels(
        source,
        keyColumns,
        channels
      )

      for await (const chan of channels) {
        const tableChunksAsyncIterable: TableChunksSource = {
          ...source,
          getHeader: () => srcHeader,
          [Symbol.asyncIterator]: chan[Symbol.asyncIterator].bind(chan)
        }

        const gen = createTableTransformer(transformConfig)(
          tableChunksAsyncIterable
        )

        try {
          for await (const chunk of gen) {
            yield chunk
          }
        } finally {
          chan.close()
        }
      }

      await pipeSourceToChannelsPromise
    }

    return {
      ...source,
      getHeader: () => transfomedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
