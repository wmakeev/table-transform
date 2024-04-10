import { DataRowChunkTransformer, TableHeaderMeta } from '../../index.js'

export interface FillColumnParams {
  columnName: string
  value: unknown
  arrIndex?: number
}

/**
 * Fill column with value
 */
export const fill = (params: FillColumnParams): DataRowChunkTransformer => {
  const { columnName, value, arrIndex } = params

  let fillColumns: TableHeaderMeta | null = null

  const factory: DataRowChunkTransformer = async ({
    header,
    rows,
    rowLength
  }) => {
    if (fillColumns === null) {
      fillColumns = header.filter(h => h.name === columnName)

      if (fillColumns.length === 0) {
        throw new Error(`Column "${columnName}" not found and can't be filled`)
      }
    }

    rows.forEach(row => {
      fillColumns!.forEach((h, index) => {
        if (typeof arrIndex === 'number' && index !== arrIndex) return

        row[h.srcIndex] = value
      })
    })

    return {
      header,
      rows,
      rowLength
    }
  }

  return factory
}
