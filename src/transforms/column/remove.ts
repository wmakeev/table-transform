import {
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableHeader } from '../../index.js'

const TRANSFORM_NAME = 'Column:Remove'

export interface RemoveColumnParams {
  column: string
  colIndex?: number
  isInternalIndex?: boolean
}

/**
 * Remove column
 */
export const remove = (params: RemoveColumnParams): TableChunksTransformer => {
  return source => {
    const { column, colIndex, isInternalIndex = false } = params

    if (isInternalIndex === true && colIndex == null) {
      throw new TransformStepError(
        'isInternalIndex is true, but colIndex is not specified',
        TRANSFORM_NAME
      )
    }

    const deletedColsSrcIndexes: number[] = []

    let headerIndex = 0

    const srcHeader = source.getTableHeader()

    const transformedHeader: TableHeader = srcHeader.flatMap(h => {
      if (!h.isDeleted && h.name === column) {
        if (
          colIndex != null
            ? colIndex === (isInternalIndex ? h.index : headerIndex)
            : true
        ) {
          deletedColsSrcIndexes.push(h.index)

          return {
            ...h,
            isDeleted: true
          }
        }

        headerIndex++
      }

      return h
    })

    if (deletedColsSrcIndexes.length === 0) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, srcHeader, [
        column
      ])
    }

    return {
      ...source,
      getTableHeader: () => transformedHeader,
      [Symbol.asyncIterator]: source[Symbol.asyncIterator]
    }
  }
}
