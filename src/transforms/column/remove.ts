import {
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Remove'

export interface RemoveColumnParams {
  columnName: string
  colIndex?: number
  isInternalIndex?: boolean
}

/**
 * Remove column
 */
export const remove = (params: RemoveColumnParams): TableChunksTransformer => {
  return source => {
    const { columnName, colIndex, isInternalIndex = false } = params

    if (isInternalIndex === true && colIndex == null) {
      throw new TransformStepError(
        'isInternalIndex is true, but colIndex is not specified',
        TRANSFORM_NAME
      )
    }

    const deletedColsSrcIndexes: number[] = []

    let headerIndex = 0

    const srcHeader = source.getHeader()

    const transformedHeader: ColumnHeader[] = srcHeader.flatMap((h, index) => {
      if (!h.isDeleted && h.name === columnName) {
        if (
          colIndex != null
            ? colIndex === (isInternalIndex ? index : headerIndex)
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
        columnName
      ])
    }

    return {
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: source[Symbol.asyncIterator]
    }
  }
}
