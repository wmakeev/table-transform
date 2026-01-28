import assert from 'node:assert'
import {
  TransformStepColumnsNotFoundError,
  TransformStepHeaderError,
  TransformStepRowError
} from '../../errors/index.js'
import { TableChunksTransformer, TableHeader, TableRow } from '../../index.js'
import { TransformBaseParams } from '../index.js'

const TRANSFORM_NAME = 'Column:Unroll'

export interface UnrollColumnParams extends TransformBaseParams {
  column: string
  strictArrayColumn?: boolean
  arrIndex?: number
}

/**
 * Unroll row
 */
export const unroll = (params: UnrollColumnParams): TableChunksTransformer => {
  const { column, strictArrayColumn = false, arrIndex = 0 } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const explodeColumns: TableHeader = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    if (explodeColumns[0] === undefined) {
      new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    const explodeColumn = explodeColumns[arrIndex]

    if (explodeColumn === undefined) {
      throw new TransformStepHeaderError(
        `Column "${column}" with index ${arrIndex} not found`,
        TRANSFORM_NAME,
        tableHeader
      )
    }

    async function* getTransformedSourceGenerator() {
      assert.ok(explodeColumn)

      for await (const chunk of source) {
        const explodedRows: TableRow[] = []

        for (const [rowIndex, row] of chunk.entries()) {
          const explodingCellValue = row[explodeColumn!.index]

          if (Array.isArray(explodingCellValue)) {
            for (const item of explodingCellValue) {
              const newRow = [...row]
              newRow[explodeColumn!.index] = item
              explodedRows.push(newRow)
            }
          } else if (strictArrayColumn === false) {
            explodedRows.push(row)
          } else {
            throw new TransformStepRowError(
              `column value expected to be array.`,
              TRANSFORM_NAME,
              tableHeader,
              chunk,
              rowIndex,
              explodeColumn.index
            )
          }
        }

        if (explodedRows.length > 0) yield explodedRows
      }
    }

    return {
      ...source,
      getTableHeader: () => source.getTableHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
