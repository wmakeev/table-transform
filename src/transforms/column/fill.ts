import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Fill'

export interface FillColumnParams {
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
    const header = source.getHeader()

    const fillColumns: ColumnHeader[] = header.filter(
      h => !h.isDeleted && h.name === column
    )

    if (fillColumns.length === 0) {
      new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
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
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
