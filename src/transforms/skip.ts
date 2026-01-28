import {
  TableChunksTransformer,
  TransformStepParameterError
} from '../index.js'
import { TransformBaseParams } from './index.js'

export interface SkipRowsParams extends TransformBaseParams {
  count: number
}

const TRANSFORM_NAME = 'Skip'

/**
 * Skip specified rows count
 */
export const skip = (params: SkipRowsParams): TableChunksTransformer => {
  const { count } = params

  if (typeof count !== 'number') {
    throw new TransformStepParameterError(
      'Expected count parameter to be number',
      TRANSFORM_NAME,
      'count',
      count
    )
  }

  if (count < 0) {
    throw new TransformStepParameterError(
      'Expected count parameter to be greater or equal to 0',
      TRANSFORM_NAME,
      'count',
      count
    )
  }

  return source => {
    const tableHeader = source.getTableHeader()

    return {
      ...source,
      getTableHeader: () => tableHeader,

      [Symbol.asyncIterator]: async function* () {
        let skippedRowsCount = 0
        let isSkipDone = false

        for await (const chunk of source) {
          if (isSkipDone) {
            yield chunk
          }
          //
          else if (skippedRowsCount + chunk.length <= count) {
            skippedRowsCount += chunk.length
            if (skippedRowsCount === count) {
              isSkipDone = true
            }
          }
          //
          else {
            yield chunk.slice(count - skippedRowsCount)
            isSkipDone = true
          }
        }
      }
    }
  }
}
