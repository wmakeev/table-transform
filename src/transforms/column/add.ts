import assert from 'assert'
import {
  DataRowChunkTransformer,
  DataRowChunkInfo,
  TableHeaderMeta
} from '../../index.js'

export interface AddColumnParams {
  columnName: string
  defaultValue?: unknown
}

/**
 * Adds new column
 */
export const add = (params: AddColumnParams): DataRowChunkTransformer => {
  const defaultValue = params.defaultValue ?? null

  let transformedHeader: TableHeaderMeta | null = null
  let newRowLength: number | null = null

  return async ({ header, rows, rowLength }) => {
    if (transformedHeader === null) {
      newRowLength = rowLength + 1

      transformedHeader = [
        ...header,
        {
          srcIndex: rowLength,
          name: params.columnName
        }
      ]
    }

    rows.forEach(row => {
      assert.equal(row.push(defaultValue), newRowLength)
    })

    const resultChunk: DataRowChunkInfo = {
      header: transformedHeader,
      rows,
      rowLength: newRowLength!
    }

    return resultChunk
  }
}
