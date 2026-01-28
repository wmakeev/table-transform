import {
  TableChunksTransformer,
  TransformStepParameterError
} from '../../index.js'
import { TransformBaseParams } from '../index.js'

export interface RebatchParams extends TransformBaseParams {
  count: number
}

const TRANSFORM_NAME = 'Internal:Rebatch'

/**
 * Rebatch internal rows chunks
 */
export const rebatch = (params: RebatchParams): TableChunksTransformer => {
  const { count } = params

  if (count <= 0) {
    throw new TransformStepParameterError(
      'Expected count parameter to be greater than 0 number',
      TRANSFORM_NAME,
      'count',
      count
    )
  }

  return source => {
    return {
      ...source,

      [Symbol.asyncIterator]: async function* () {
        let batch = Array(count)
        let batchLen = 0

        for await (const chunk of source) {
          if (chunk.length === count && batchLen === 0) {
            yield chunk
            continue
          }

          for (const row of chunk) {
            if (batchLen === count) {
              yield batch
              batch = Array(count)
              batchLen = 0
            }

            batch[batchLen] = row
            batchLen++
          }
        }

        if (batchLen !== 0) {
          yield batch.slice(0, batchLen)
        }
      }
    }
  }
}
