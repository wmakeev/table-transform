import {
  DataRowChunkTransformer,
  DataRowChunkInfo,
  TableHeaderMeta
} from '../../index.js'

export interface RemoveColumnParams {
  columnName: string
  colIndex?: number
  eraseData?: boolean
}

/**
 * Remove column
 */
export const remove = (params: RemoveColumnParams): DataRowChunkTransformer => {
  let transformedHeader: TableHeaderMeta | null = null

  const deletedColsSrcIndexes: number[] = []

  return async ({ header, rows, rowLength }) => {
    if (transformedHeader === null) {
      let headerIndex = 0

      transformedHeader = header.flatMap(h => {
        if (h.name === params.columnName) {
          if (
            params.colIndex != null ? params.colIndex === headerIndex : true
          ) {
            deletedColsSrcIndexes.push(h.srcIndex)

            return []
          }

          headerIndex++
        }

        return h
      })

      if (deletedColsSrcIndexes.length === 0) {
        throw new Error(
          `Column "${params.columnName}" not found and can't be removed`
        )
      }
    }

    if (params.eraseData === true) {
      rows.forEach(row => {
        deletedColsSrcIndexes.forEach(index => {
          row[index] = null
        })
      })
    }

    const resultChunk: DataRowChunkInfo = {
      header: transformedHeader,
      rows,
      rowLength
    }

    return resultChunk
  }
}
