import assert from 'assert'
import { TransformStepError } from '../errors/index.js'
import {
  TableChunksAsyncIterable,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { getNormalizedHeaderRow } from '../tools/header/index.js'

export interface SplitInParams {
  splitColumns: string[]
  transformConfig: TableTransfromConfig
}

const TRANSFORM_NAME = 'SplitIn'

const getRowsComparator = (columnIndexes: number[]) => {
  return (a: TableRow, b: TableRow): boolean => {
    return columnIndexes.every(index => a[index] === b[index])
  }
}

const createRowIndexNamer =
  (splitColumnIndexes: number[]) => (row: TableRow) => {
    return splitColumnIndexes.map(i => String(row[i])).join()
  }

async function pipeSourceToChannels(
  source: TableChunksAsyncIterable,
  targetChannelsChan: AsyncChannel<AsyncChannel<TableRow[]>>,
  splitColumns: string[]
): Promise<void> {
  const splitColumnsSet = new Set(splitColumns)

  const splitColumnIndexes = source
    .getHeader()
    .filter(h => !h.isDeleted && splitColumnsSet.has(h.name))
    .map(h => h.index)

  const getRowIndexName = createRowIndexNamer(splitColumnIndexes)

  const rowsComparator = getRowsComparator(splitColumnIndexes)

  let chan: AsyncChannel<TableRow[]>

  let curPartRowSample: TableRow | undefined = undefined

  for await (const chunk of source) {
    // on iteration start
    if (curPartRowSample === undefined) {
      curPartRowSample = chunk[0]
      chan = new AsyncChannel<TableRow[]>({
        name: `${TRANSFORM_NAME}:chan("${getRowIndexName(curPartRowSample!)}")`
      })
      await targetChannelsChan.put(chan)
    }

    let chanCurIndex = 0

    while (true) {
      let chanSplitIndex = -1

      for (let i = chanCurIndex; i < chunk.length; i++) {
        if (!rowsComparator(curPartRowSample!, chunk[i]!)) {
          chanSplitIndex = i
          break
        }
      }

      assert.ok(chan!)

      if (chanSplitIndex === -1) {
        if (!chan.isClosed())
          await chan.put(chanCurIndex === 0 ? chunk : chunk.slice(chanCurIndex))

        break
      }

      curPartRowSample = [...chunk[chanSplitIndex]!]

      if (!chan.isClosed()) {
        if (chanSplitIndex !== 0) {
          await chan.put(
            chanSplitIndex !== chanCurIndex
              ? chunk.slice(chanCurIndex, chanSplitIndex)
              : chanCurIndex === 0
                ? chunk
                : chunk.slice(chanCurIndex)
          )
        }

        if (!chan.isFlushed()) await chan.flush()
        chan.close()
      }

      chanCurIndex = chanSplitIndex

      chan = new AsyncChannel<TableRow[]>({
        name: `${TRANSFORM_NAME}:chan("${getRowIndexName(curPartRowSample!)}")`
      })
      await targetChannelsChan.put(chan)
    }
  }

  if (!chan!.isFlushed()) await chan!.flush()
  chan!.close()

  if (!targetChannelsChan.isFlushed()) await targetChannelsChan.flush()
  targetChannelsChan.close()
}

/**
 * SplitIn
 */
export const splitIn = (params: SplitInParams): TableChunksTransformer => {
  const { splitColumns } = params

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

      for (const col of splitColumns) {
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

      const p = pipeSourceToChannels(source, channels, splitColumns)
      // .catch(
      //   async err => {
      //     console.error(err)
      //   }
      // )

      for await (const chan of channels) {
        const tableChunksAsyncIterable: TableChunksAsyncIterable = {
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
        } catch (err) {
          console.log(err)
          throw err
        } finally {
          chan.close()
        }
      }

      await p
    }

    return {
      getHeader: () => transfomedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
