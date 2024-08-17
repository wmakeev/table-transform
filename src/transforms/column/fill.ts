import { TransformStepError } from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Fill'

export interface FillColumnParams {
  columnName: string
  value: unknown
  arrIndex?: number
}

/**
 * Fill column with value
 */
export const fill = (params: FillColumnParams): TableChunksTransformer => {
  const { columnName, value, arrIndex } = params

  return source => {
    const fillColumns: ColumnHeader[] = source
      .getHeader()
      .filter(h => h.name === columnName)

    if (fillColumns.length === 0) {
      throw new TransformStepError(
        `Column "${columnName}" not found and can't be filled`,
        TRANSFORM_NAME
      )
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
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
