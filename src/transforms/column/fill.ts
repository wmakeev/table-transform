import { TransformStepColumnsNotFoundError } from '../../errors/index.js'
import { TableChunksTransformer, TableHeader } from '../../index.js'
import { TransformBaseParams } from '../index.js'

const TRANSFORM_NAME = 'Column:Fill'

export interface FillColumnParams extends TransformBaseParams {
  column: string
  value: unknown
  arrIndex?: number
}

/**
 * Fill column with value
 */
export const fill = (params: FillColumnParams): TableChunksTransformer => {
  const { column, value, arrIndex } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const fillColumns: TableHeader = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    if (fillColumns.length === 0) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        chunk.forEach(row => {
          fillColumns.forEach((h, index) => {
            if (typeof arrIndex === 'number' && index !== arrIndex) return
            row[h.index] = value
          })
        })

        yield chunk
      }
    }

    return {
      ...source,
      getTableHeader: () => source.getTableHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
