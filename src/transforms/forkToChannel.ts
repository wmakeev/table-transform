import assert from 'assert'
import {
  AsyncChannel,
  cloneChunk,
  HeaderChunkTuple,
  TableChunksTransformer
} from '../index.js'
import { channelScopeSymbol, TransformBaseParams } from './index.js'

export interface ForkToChannelParams extends TransformBaseParams {
  channelName: string
}

/**
 * Sends a copy of the stream to the channel
 */
export const forkToChannel = (
  params: ForkToChannelParams
): TableChunksTransformer => {
  const { channelName } = params

  return src => {
    const tableHeader = src.getTableHeader()
    const context = src.getContext()

    let channel = context.get(channelScopeSymbol, channelName) as
      | AsyncChannel<HeaderChunkTuple>
      | undefined

    if (channel == null) {
      channel = new AsyncChannel<HeaderChunkTuple>()
      context.set(channelScopeSymbol, channelName, channel)
    }

    assert.ok(channel instanceof AsyncChannel, 'AsyncChannel instance expected')

    return {
      ...src,
      [Symbol.asyncIterator]: async function* () {
        let prevPutPromise: Promise<boolean> | undefined = undefined

        for await (const chunk of src) {
          if (prevPutPromise != null) {
            await prevPutPromise
            prevPutPromise = undefined
          }

          if (!channel.isClosed()) {
            prevPutPromise = channel.put([tableHeader, cloneChunk(chunk)])
          }

          yield chunk
        }

        if (prevPutPromise != null) {
          await prevPutPromise
        }

        if (!channel.isClosed()) {
          if (!channel.isFlushed()) await channel.flush()
          channel.close()
        }
      }
    }
  }
}
