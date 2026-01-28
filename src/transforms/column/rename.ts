import { TransformStepColumnsNotFoundError } from '../../errors/index.js'
import { TableChunksTransformer, TableHeader } from '../../index.js'
import { TransformBaseParams } from '../index.js'

const TRANSFORM_NAME = 'Column:Rename'

export interface RenameColumnParams extends TransformBaseParams {
  column: string
  newName: string
}

/**
 * Rename header
 */
export const rename = (params: RenameColumnParams): TableChunksTransformer => {
  return source => {
    let isColumnFound = false

    const srcHeader = source.getTableHeader()

    const transformedHeader: TableHeader = srcHeader.map(h => {
      if (!h.isDeleted && h.name === params.column) {
        isColumnFound = true

        return {
          ...h,
          name: params.newName
        }
      }

      return h
    })

    if (!isColumnFound) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, srcHeader, [
        params.column
      ])
    }

    return {
      ...source,
      getTableHeader: () => transformedHeader,
      [Symbol.asyncIterator]: () => source[Symbol.asyncIterator]()
    }
  }
}
