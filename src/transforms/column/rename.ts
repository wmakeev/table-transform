import { ColumnHeader, TableChunksTransformer } from '../../index.js'

export interface RenameColumnParams {
  oldColumnName: string
  newColumnName: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): TableChunksTransformer => {
  return async ({ header, getSourceGenerator }) => {
    let isHeaderFound = false

    const transformedHeaders: ColumnHeader[] = header.map(h => {
      if (!h.isDeleted && h.name === params.oldColumnName) {
        isHeaderFound = true

        return {
          ...h,
          name: params.newColumnName
        }
      }

      return h
    })

    if (!isHeaderFound) {
      throw new Error(
        `Header "${params.oldColumnName}" not found and can't be renamed`
      )
    }

    return {
      header: transformedHeaders,
      getSourceGenerator
    }
  }
}
