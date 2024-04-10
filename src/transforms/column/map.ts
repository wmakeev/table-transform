import { DataRowChunkTransformer, TableHeaderMeta } from '../../index.js'

interface MapColumnParams {
  columnName: string
  mapper: (value: unknown) => unknown
  arrIndex?: number
}

/**
 * Map column values with specified function
 */
export const map = (params: MapColumnParams): DataRowChunkTransformer => {
  const { columnName, mapper, arrIndex } = params

  let mappingColumns: TableHeaderMeta | null = null

  const factory: DataRowChunkTransformer = async ({
    header,
    rows,
    rowLength
  }) => {
    if (mappingColumns === null) {
      mappingColumns = header.filter(h => h.name === columnName)
    }

    rows.forEach(row => {
      mappingColumns!.forEach((h, index) => {
        if (typeof arrIndex === 'number' && index !== arrIndex) return

        const value = row[h.srcIndex]

        row[h.srcIndex] = mapper(value)
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
