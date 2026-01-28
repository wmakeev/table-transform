import { TransformStepError } from '../errors/index.js'
import {
  Context,
  TableChunksSource,
  TableChunksTransformer,
  TableRow,
  TableTransformConfig,
  createTableTransformer,
  splitChunksBy,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { getNormalizedHeaderRow } from '../tools/header/index.js'
import { TransformBaseParams } from './index.js'

export interface SplitInParams extends TransformBaseParams {
  /**
   * The columns by which the incoming stream is splitted
   */
  keyColumns: string[]

  /**
   * The config of transformation through which each of the splitted parts of
   * the stream is passed.
   */
  transformConfig: TableTransformConfig
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
    const srcHeader = source.getTableHeader()

    // Drop headers from splits
    const transformConfig: TableTransformConfig = {
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
    // вызывать повторно?), но выглядит это запутанно и очень сложно отлаживать.

    const transforms_ = [...(transformConfig.transforms ?? [])]

    // Ensure all forced columns exist and select
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

    const transformedHeader = tableSource.getTableHeader()
    //#endregion

    async function* getTransformedSourceGenerator() {
      const normalizedHeaderColumns = getNormalizedHeaderRow(srcHeader)

      // TODO Выделить в отдельный helper?
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
        const tableChunksSource: TableChunksSource = {
          ...source,
          getTableHeader: () => srcHeader,
          [Symbol.asyncIterator]: chan[Symbol.asyncIterator].bind(chan)
        }

        const gen = createTableTransformer({
          context: new Context(source.getContext()),
          ...transformConfig
        })(tableChunksSource)

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
      getTableHeader: () => transformedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
