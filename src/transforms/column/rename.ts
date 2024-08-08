import { ColumnHeader, TableChunksTransformer } from '../../index.js'

export interface RenameColumnParams {
  oldColumnName: string
  newColumnName: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): TableChunksTransformer => {
  return source => {
    let isHeaderFound = false

    const transformedHeader: ColumnHeader[] = source.getHeader().map(h => {
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
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: () => source[Symbol.asyncIterator]()
    }
  }
}
