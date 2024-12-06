import assert from 'assert'
import {
  AsyncChannel,
  Context,
  createTableTransformer,
  HeaderChunkTuple,
  pipeAsyncIterableToChannel,
  TableChunksSource,
  TableChunksTransformer,
  TableRow,
  transforms as tf
} from '../index.js'
import { channelScopeSymbol, normalize } from './index.js'

const TRANSFORM_NAME = 'MergeFromChannel'

export interface MergeFromChannelParams {
  channelName: string
}

async function* sourceChanTransformer(
  outputColumns: string[],
  srcChan: AsyncChannel<HeaderChunkTuple>
) {
  const emptyContext = new Context()

  for await (const [header, chunk] of srcChan) {
    const chanTransformer = createTableTransformer({
      transforms: [
        tf.column.select({ columns: outputColumns, addMissingColumns: true })
      ],
      outputHeader: {
        skip: true
      }
    })

    const chunkSource: TableChunksSource = {
      getHeader: () => header,
      getContext: () => emptyContext,
      async *[Symbol.asyncIterator]() {
        yield chunk
      }
    }

    for await (const chunk of chanTransformer(chunkSource)) {
      yield chunk
    }
  }
}

/**
 * Merge channel content to the stream
 */
export const mergeFromChannel = (
  params: MergeFromChannelParams
): TableChunksTransformer => {
  const { channelName } = params

  return source => {
    const normalizedSource = normalize()(source)

    const outputColumns = normalizedSource
      .getHeader()
      .filter(h => !h.isDeleted)
      .map(h => h.name)

    const context = normalizedSource.getContext()

    let channel = context.get(channelScopeSymbol, channelName) as
      | AsyncChannel<HeaderChunkTuple>
      | undefined

    if (channel == null) {
      channel = new AsyncChannel<HeaderChunkTuple>()
      context.set(channelScopeSymbol, channelName, channel)
    }

    assert.ok(channel instanceof AsyncChannel, 'AsyncChannel instance expected')

    return {
      ...normalizedSource,
      [Symbol.asyncIterator]: async function* () {
        const mergeChan = new AsyncChannel<TableRow[]>({
          name: `${TRANSFORM_NAME}:mergeChan`
        })

        const sourceConsumerPromise = pipeAsyncIterableToChannel(
          normalizedSource,
          mergeChan
        )

        const channelConsumerPromise = pipeAsyncIterableToChannel(
          sourceChanTransformer(outputColumns, channel),
          mergeChan
        )

        Promise.all([sourceConsumerPromise, channelConsumerPromise]).then(
          () => {
            mergeChan.close()
          }
        )

        for await (const chunk of mergeChan) {
          yield chunk
        }
      }
    }
  }
}
