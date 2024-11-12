import {
  TableChunksTransformer,
  TransformColumnsNotFoundError
} from '../../index.js'
import { probesMapPropSymbol } from './index.js'

interface ProbePutColumnParams {
  key: string
  column: string
  arrIndex?: number
}

const TRANSFORM_NAME = 'Column:ProbePut'

/**
 * Fill column with early probed value
 */
export const probePut = (
  params: ProbePutColumnParams
): TableChunksTransformer => {
  const { key, column } = params

  return source => {
    const header = source.getHeader()

    const colHeader = header.find(h => !h.isDeleted && h.name === column)

    if (colHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        const colIndex = colHeader.index

        let isProbeTaked = false

        let probedValue: unknown

        for await (const chunk of source) {
          if (isProbeTaked === false) {
            const probesMap = source
              .getContext()
              .getValue(probesMapPropSymbol) as Map<string, unknown>

            probedValue = probesMap != null ? probesMap.get(key) : undefined

            isProbeTaked = true
          }

          for (const row of chunk) {
            row[colIndex] = probedValue
          }

          yield chunk
        }
      }
    }
  }
}
