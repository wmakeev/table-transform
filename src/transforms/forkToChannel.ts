import {
  AsyncChannel,
  cloneChunk,
  HeaderChunkTuple,
  TableChunksTransformer
} from '../index.js'

export interface ForkToChannelParams {
  channel: AsyncChannel<HeaderChunkTuple>
}

/**
 * Sends a copy of the stream to the channel
 */
export const forkToChannel = (
  params: ForkToChannelParams
): TableChunksTransformer => {
  const { channel } = params

  return src => {
    const header = src.getHeader()

    return {
      ...src,
      [Symbol.asyncIterator]: async function* () {
        let prevPutPromise: Promise<boolean> | undefined = undefined

        for await (const chunk of src) {
          if (prevPutPromise != null) {
            await prevPutPromise
          }

          if (!channel.isClosed()) {
            prevPutPromise = channel.put([header, cloneChunk(chunk)])
          }

          yield chunk
        }

        if (!channel.isClosed()) {
          if (!channel.isFlushed()) await channel.flush()
          channel.close()
        }
      }
    }
  }
}
