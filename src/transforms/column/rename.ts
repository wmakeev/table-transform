import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Rename'

export interface RenameColumnParams {
  oldColumn: string
  newColumn: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): TableChunksTransformer => {
  return source => {
    let isColumnFound = false

    const srcHeader = source.getHeader()

    const transformedHeader: ColumnHeader[] = srcHeader.map(h => {
      if (!h.isDeleted && h.name === params.oldColumn) {
        isColumnFound = true

        return {
          ...h,
          name: params.newColumn
        }
      }

      return h
    })

    if (!isColumnFound) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, srcHeader, [
        params.oldColumn
      ])
    }

    return {
      ...source,
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: () => source[Symbol.asyncIterator]()
    }
  }
}
