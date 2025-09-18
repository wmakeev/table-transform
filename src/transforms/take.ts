import {
  TableChunksTransformer,
  TransformStepParameterError
} from '../index.js'

export interface TakeRowsParams {
  count: number
}

const TRANSFORM_NAME = 'Take'

/**
 * Take specified rows count
 */
export const take = (params: TakeRowsParams): TableChunksTransformer => {
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
    const tableHeader = source.getTableHeader()

    return {
      ...source,
      getTableHeader: () => tableHeader,

      [Symbol.asyncIterator]: async function* () {
        let takenRowsCount = 0

        for await (const chunk of source) {
          if (takenRowsCount + chunk.length <= count) {
            takenRowsCount += chunk.length
            yield chunk
            if (takenRowsCount === count) break
          } else {
            yield chunk.slice(0, count - takenRowsCount)
            break
          }
        }
      }
    }
  }
}
