import { ColumnHeader, TableChunksTransformer } from '../../index.js'

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

  return async ({ header, getSourceGenerator }) => {
    const fillColumns: ColumnHeader[] = header.filter(
      h => h.name === columnName
    )

    if (fillColumns.length === 0) {
      throw new Error(`Column "${columnName}" not found and can't be filled`)
    }

    async function* getTransformedSourceGenerator() {
      for await (const chunk of getSourceGenerator()) {
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
      header,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
