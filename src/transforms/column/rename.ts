import { ColumnHeaderMeta, DataRowChunkTransformer } from '../../index.js'

export interface RenameColumnParams {
  oldColumnName: string
  newColumnName: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): DataRowChunkTransformer => {
  let transformedHeaders: ColumnHeaderMeta[] | null = null

  return async ({ header, rows, rowLength }) => {
    if (transformedHeaders === null) {
      let isHeaderFound = false

      transformedHeaders = header.map(h => {
        if (h.name === params.oldColumnName) {
          isHeaderFound = true

          return {
            ...h,
            name: params.newColumnName
          }
        } else {
          return h
        }
      })

      if (!isHeaderFound) {
        throw new Error(
          `Header "${params.oldColumnName}" not found and can't be renamed`
        )
      }
    }

    return {
      header: transformedHeaders,
      rows,
      rowLength
    }
  }
}
