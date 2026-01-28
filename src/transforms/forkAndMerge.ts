import {
  Context,
  TableChunksSource,
  TableChunksTransformer,
  TableHeader,
  TableRow,
  TableTransformConfig,
  cloneChunk,
  createTableTransformer,
  pipeAsyncIterableToChannel,
  transforms as tf
} from '../index.js'
import { AsyncChannel } from '../tools/AsyncChannel/index.js'
import { TransformBaseParams } from './index.js'
import { normalize } from './internal/index.js'

// TODO Можно ли оптимизировать копирование chunk в первый fork без клонирования?

const TRANSFORM_NAME = 'ForkAndMerge'

export interface ForkAndMergeParams extends TransformBaseParams {
  outputColumns: string[]
  transformConfigs: TableTransformConfig[]
}

async function flushAndCloseChannels(channels: AsyncChannel<any>[]) {
  await Promise.all(
    channels.map(async (chan): Promise<void> => {
      await chan.flush()
      chan.close()
    })
  )
}

async function forkProducer(
  source: TableChunksSource,
  forkedChans: AsyncChannel<TableRow[]>[]
) {
  const normalizedSource = normalize({ immutable: false })(source)

  const srcHeader = normalizedSource.getTableHeader()

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

/**
 * Fork and merge
 */
export const forkAndMerge = (
  params: ForkAndMergeParams
): TableChunksTransformer => {
  const { outputColumns, transformConfigs } = params

  return source => {
    const resultHeader: TableHeader = outputColumns.map((name, index) => ({
      index,
      name: String(name),
      isDeleted: false
    }))

    async function* getTransformedSourceGenerator() {
      const forkedChans = transformConfigs.map(
        (_, index) =>
          new AsyncChannel<TableRow[]>({
            name: `${TRANSFORM_NAME}:forkedChans[${index}]`
          })
      )

      const mergeChan = new AsyncChannel<TableRow[]>({
        name: `${TRANSFORM_NAME}:mergeChan`
      })

      const transformGens = transformConfigs.map((forkConfig, index) => {
        const forkTransform = createTableTransformer({
          context: new Context(source.getContext()),
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
          pipeAsyncIterableToChannel(transformGen, mergeChan)
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
      ...source,
      getTableHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
