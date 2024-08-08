import { ColumnHeader, TableChunksTransformer } from '../../index.js'

export interface RemoveColumnParams {
  columnName: string
  colIndex?: number
  clearColumn?: boolean
}

/**
 * Remove column
 */
export const remove = (params: RemoveColumnParams): TableChunksTransformer => {
  return source => {
    const deletedColsSrcIndexes: number[] = []

    let headerIndex = 0

    const transformedHeader: ColumnHeader[] = source.getHeader().flatMap(h => {
      if (!h.isDeleted && h.name === params.columnName) {
        if (params.colIndex != null ? params.colIndex === headerIndex : true) {
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
      throw new Error(
        `Column "${params.columnName}" not found and can't be removed`
      )
    }

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        if (params.clearColumn === true) {
          chunk.forEach(row => {
            deletedColsSrcIndexes.forEach(index => {
              row[index] = null
            })
          })
        }

        yield chunk
      }
    }

    return {
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
