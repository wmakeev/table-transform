import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Rename'

export interface RenameColumnParams {
  oldColumnName: string
  newColumnName: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): TableChunksTransformer => {
  return source => {
    let isColumnFound = false

    const srcHeader = source.getHeader()

    const transformedHeader: ColumnHeader[] = srcHeader.map(h => {
      if (!h.isDeleted && h.name === params.oldColumnName) {
        isColumnFound = true

        return {
          ...h,
          name: params.newColumnName
        }
      }

      return h
    })

    if (!isColumnFound) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, srcHeader, [
        params.oldColumnName
      ])
    }

    return {
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: () => source[Symbol.asyncIterator]()
    }
  }
}
