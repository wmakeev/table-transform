import {
  ColumnHeader,
  TableChunksAsyncIterable,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  cloneChunk,
  createTableTransformer,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { normalize } from './normalize.js'

export interface MergeForkParams {
  outputColumns: string[]
  transformConfigs: TableTransfromConfig[]
}

// const TRANSFORM_NAME = 'MergeFork'

async function flushAndCloseChannels(channels: AsyncChannel<any>[]) {
  await Promise.all(
    channels.map(async (chan): Promise<void> => {
      await chan.flush()
      chan.close()
    })
  )
}

async function forkProducer(
  source: TableChunksAsyncIterable,
  forkedChans: AsyncChannel<TableRow[]>[]
) {
  const normalizedSource = normalize({ immutable: false })(source)

  const srcHeader = normalizedSource.getHeader()

  const colNames = srcHeader.map(h => h.name)

  // Put headers to forks
  const headersPutResults = await Promise.all(
    forkedChans.map(ch => {
      if (ch.isClosed()) return false
      return ch.put([[...colNames]])
    })
  )

  // Not all forks is closed
  if (headersPutResults.some(it => it === true)) {
    // Multiplex source data to forks
    for await (const chunk of normalizedSource) {
      const chunkPutResults = await Promise.all(
        forkedChans.map(ch => ch.put(cloneChunk(chunk)))
      )

      // all fork is closed
      if (!chunkPutResults.some(it => it === true)) break
    }
  }

  // Flush close forked channels (closed channel can have pending readers )
  await flushAndCloseChannels(forkedChans)
}

async function pipeGenToChanAsync(
  sourceGen: AsyncGenerator<TableRow[]>,
  targetChan: AsyncChannel<TableRow[]>
) {
  for await (const chunk of sourceGen) {
    if (targetChan.isClosed()) break
    await targetChan.put(chunk)
  }

  await targetChan.flush()
}

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
      const forkedChans = transformConfigs.map(
        () => new AsyncChannel<TableRow[]>()
      )

      const mergeChan = new AsyncChannel<TableRow[]>()

      const transformGens = transformConfigs.map((forkConfig, index) => {
        const forkTransform = createTableTransformer({
          ...forkConfig,
          transforms: [
            ...(forkConfig.transforms ?? []),
            tf.column.select({
              columns: outputColumns,
              addMissingColumns: true
            })
          ],
          outputHeader: {
            ...forkConfig.outputHeader,
            // TODO См. выше forceColumns
            skip: true
          }
        })

        return forkTransform(forkedChans[index]!)
      })

      // Merge consumer
      Promise.all(
        transformGens.map(transformGen =>
          pipeGenToChanAsync(transformGen, mergeChan)
        )
      ).then(() => {
        mergeChan.close()
      })

      // Fork consumer
      forkProducer(source, forkedChans)

      for await (const chunk of mergeChan) {
        yield chunk
      }
    }

    return {
      getHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
