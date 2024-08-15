import { ColumnHeader, TableChunksTransformer } from '../../index.js'

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
      throw new Error('isInternalIndex is true, but colIndex is not specified')
    }

    const deletedColsSrcIndexes: number[] = []

    let headerIndex = 0

    const transformedHeader: ColumnHeader[] = source
      .getHeader()
      .flatMap((h, index) => {
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
      throw new Error(`Column "${columnName}" not found and can't be removed`)
    }

    return {
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: source[Symbol.asyncIterator]
    }
  }
}
