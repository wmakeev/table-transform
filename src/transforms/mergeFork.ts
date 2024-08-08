import {
  ColumnHeader,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { getChunkNormalizer } from '../tools/headers.js'
//import { setTimeout as setTimeoutAsync } from 'node:timers/promises'

export interface MergeForkParams {
  outputColumns: string[]
  transformConfigs: TableTransfromConfig[]
}

// const TRANSFORM_NAME = 'MergeFork'

/**
 * MergeFork
 */
export const mergeFork = (params: MergeForkParams): TableChunksTransformer => {
  const { outputColumns, transformConfigs } = params

  return source => {
    const resultHeader: ColumnHeader[] = outputColumns.map((name, index) => ({
      index,
      name: String(name),
      isDeleted: false
    }))

    async function* getTransformedSourceGenerator() {
      const srcHeader = source.getHeader()

      const normalizeRowsChunk = getChunkNormalizer(srcHeader)

      const forkedChans = transformConfigs.map(
        () => new AsyncChannel<TableRow[]>()
      )

      const mergedChan = new AsyncChannel<TableRow[]>()

      async function forkProducer() {
        const colNames = srcHeader.filter(h => !h.isDeleted).map(h => h.name)

        // put headers
        for (const forkedChan of forkedChans) {
          const headerChunk = [[...colNames]]
          const isOpen = await forkedChan.put(headerChunk)
          if (!isOpen) return
        }

        for await (const chunk of source) {
          let allClosed = true

          for (const forkedChan of forkedChans) {
            const normalizedRow = normalizeRowsChunk(chunk)
            const isOpen = await forkedChan.put(normalizedRow)
            if (isOpen) allClosed = false
          }

          if (allClosed) break
        }

        // close forked channels
        await Promise.all(
          forkedChans.map(async (chan): Promise<void> => {
            await chan.flush()
            chan.close()
          })
        )
      }

      async function mergeConsumer(gen: AsyncGenerator<TableRow[]>) {
        for await (const chunk of gen) {
          if (mergedChan.isClosed()) break
          await mergedChan.put(chunk)
        }

        await mergedChan.flush()
      }

      const transformGens = transformConfigs.map((forkConfig, index) => {
        const forkTransform = createTableTransformer({
          ...forkConfig,
          transforms: [
            ...(forkConfig.transforms ?? []),

            // TODO Не удобно. Нужно учитывать outputHeader.forceColumns и
            // errorHandle.outputColumns. Легко запутаться.
            // Нужно указывать колонки ошибок в outputHeader.forceColumns,
            // либо они будут отбрасываться.
            ...outputColumns.map(columnName => tf.column.add({ columnName })),
            tf.column.select({ columns: outputColumns })
          ],
          outputHeader: {
            ...forkConfig.outputHeader,
            // TODO См. выше forceColumns
            skip: true
          }
        })

        return forkTransform(forkedChans[index]!)
      })

      Promise.all(
        transformGens.map(transformGen => mergeConsumer(transformGen))
      ).finally(() => {
        mergedChan.close()
      })

      forkProducer()

      for await (const chunk of mergedChan) {
        yield chunk
      }
    }

    return {
      getHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
