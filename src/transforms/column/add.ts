import assert from 'assert'
import {
  ColumnHeader,
  TableChunksSource,
  TableChunksTransformer
} from '../../index.js'

export interface AddColumnParams {
  columnName: string
  defaultValue?: unknown
}

/**
 * Adds new column
 */
export const add = (params: AddColumnParams): TableChunksTransformer => {
  const defaultValue = params.defaultValue ?? null

  return async ({ header, getSourceGenerator }) => {
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
      for await (const chunk of getSourceGenerator()) {
        chunk.forEach(row => {
          const newLen = row.push(defaultValue)
          assert.equal(newLen, transformedHeader.length)
        })

        yield chunk
      }
    }

    const resultChunk: TableChunksSource = {
      header: transformedHeader,
      getSourceGenerator: getTransformedSourceGenerator
    }

    return resultChunk
  }
}
