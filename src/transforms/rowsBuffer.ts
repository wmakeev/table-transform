import assert from 'assert'
import {
  AsyncChannel,
  TableChunksSource,
  TableChunksTransformer,
  TableRow,
  TransformStepParameterError
} from '../index.js'
import { TransformBaseParams } from './index.js'

export interface BufferParams extends TransformBaseParams {
  maxBufferSize: number
}

const TRANSFORM_NAME = 'RowsBuffer'

const consumer = async (
  source: TableChunksSource,
  channel: AsyncChannel<TableRow>
) => {
  try {
    for await (const chunk of source) {
      for (const row of chunk) {
        if ((await channel.put(row)) === false) break
      }

      if (channel.isClosed()) break
    }
  } catch (err) {
    assert.ok(err instanceof Error)
    channel.close(err)
  } finally {
    if (!channel.isFlushed()) await channel.flush()
    channel.close()
  }
}

/**
 * Buffering income rows stream
 */
export const rowsBuffer = (params: BufferParams): TableChunksTransformer => {
  const { maxBufferSize } = params

  if (maxBufferSize < 0) {
    throw new TransformStepParameterError(
      'Expected maxBufferSize parameter to be positive number',
      TRANSFORM_NAME,
      'maxBufferSize',
      maxBufferSize
    )
  }

  return source => {
    const tableHeader = source.getTableHeader()

    return {
      ...source,
      getTableHeader: () => tableHeader,

      [Symbol.asyncIterator]: async function* () {
        const rowsChan = new AsyncChannel<TableRow>({
          name: `${TRANSFORM_NAME}/AsyncChannel`,
          bufferLength: maxBufferSize
        })

        consumer(source, rowsChan)

        for await (const row of rowsChan) {
          yield [row]
        }
      }
    }
  }
}
