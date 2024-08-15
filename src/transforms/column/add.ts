import { TransformBugError } from '../../errors/index.js'
import {
  ColumnHeader,
  TableChunksAsyncIterable,
  TableChunksTransformer
} from '../../index.js'

export interface AddColumnParams {
  columnName: string

  defaultValue?: unknown

  /**
   * If the adding column(s) already exists, then add a new one with the same
   * name.
   */
  force?: boolean
}

/**
 * Adds new column
 */
export const add = (params: AddColumnParams): TableChunksTransformer => {
  const defaultValue = params.defaultValue ?? null

  return source => {
    const header = source.getHeader()

    // Skip column adding?
    if (
      params.force !== true &&
      header.findIndex(h => !h.isDeleted && h.name === params.columnName) !== -1
    ) {
      return source
    }

    const firstDeletedHeaderIndex = header.findIndex(h => h.isDeleted)

    // Add a cell at the end of a row or reuse a deleted cell
    const transformedHeader: ColumnHeader[] =
      firstDeletedHeaderIndex === -1
        ? [
            ...header,
            {
              index: header.length,
              name: params.columnName,
              isDeleted: false
            }
          ]
        : header.map(h =>
            h.index === firstDeletedHeaderIndex
              ? {
                  index: firstDeletedHeaderIndex,
                  name: params.columnName,
                  isDeleted: false
                }
              : h
          )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        chunk.forEach(row => {
          if (firstDeletedHeaderIndex === -1) {
            row.push(defaultValue)
          } else {
            row[firstDeletedHeaderIndex] = defaultValue
          }

          if (row.length !== transformedHeader.length) {
            throw new TransformBugError('Row length not satisfies header')
          }
        })

        yield chunk
      }
    }

    const resultChunk: TableChunksAsyncIterable = {
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }

    return resultChunk
  }
}
