import {
  AsyncChannel,
  Context,
  createTableTransformer,
  getChunkNormalizer,
  TableChunksSource,
  TableChunksTransformer,
  TableRow,
  TableTransformConfig
} from '../index.js'
import { TransformBaseParams } from './index.js'

export interface ForkParams extends TransformBaseParams {
  transformConfig: TableTransformConfig
}

/**
 * Fork stream
 */
export const fork = (params: ForkParams): TableChunksTransformer => {
  const { transformConfig } = params

  return src => {
    const tableHeader = src.getTableHeader()

    const forkTransformer = createTableTransformer({
      context: new Context(src.getContext()),
      ...transformConfig
    })

    const {
      normalizedHeader: forkHeader,
      chunkNormalizer: forkChunkNormalizer
    } = getChunkNormalizer(tableHeader, true)

    const forkChan = new AsyncChannel<TableRow[]>()

    const forkContext = new Context(src.getContext())

    const forkSource: TableChunksSource = {
      getTableHeader: () => forkHeader,
      getContext: () => forkContext,
      async *[Symbol.asyncIterator]() {
        for await (const chunk of forkChan) {
          yield chunk
        }
      }
    }

    const consumeFork = async () => {
      for await (const _ of forkTransformer(forkSource)) _
    }

    return {
      ...src,
      async *[Symbol.asyncIterator]() {
        const consumeForkPromise = consumeFork()

        let prevPutPromise: Promise<boolean> | undefined = undefined

        for await (const chunk of src) {
          if (prevPutPromise != null) {
            await prevPutPromise
            prevPutPromise = undefined
          }

          if (!forkChan.isClosed()) {
            prevPutPromise = forkChan.put(forkChunkNormalizer(chunk))
          }

          yield chunk
        }

        if (prevPutPromise != null) {
          await prevPutPromise
        }

        if (!forkChan.isClosed()) {
          if (!forkChan.isFlushed()) await forkChan.flush()
          forkChan.close()
        }

        await consumeForkPromise
      }
    }
  }
}
