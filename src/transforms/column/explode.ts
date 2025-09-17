import assert from 'node:assert'
import {
  TransformColumnsNotFoundError,
  TransformHeaderError,
  TransformRowError
} from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer, TableRow } from '../../index.js'

const TRANSFORM_NAME = 'Column:Explode'

export interface ExplodeColumnParams {
  column: string
  strictArrayColumn?: boolean
  arrIndex?: number
}

/**
 * Explode row
 */
export const explode = (
  params: ExplodeColumnParams
): TableChunksTransformer => {
  const { column, strictArrayColumn = false, arrIndex = 0 } = params

  return source => {
    const header = source.getHeader()

    const explodeColumns: ColumnHeader[] = header.filter(
      h => !h.isDeleted && h.name === column
    )

    if (explodeColumns[0] === undefined) {
      new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    const explodeColumn = explodeColumns[arrIndex]

    if (explodeColumn === undefined) {
      throw new TransformHeaderError(
        `Column "${column}" with index ${arrIndex} not found`,
        TRANSFORM_NAME,
        header
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
            throw new TransformRowError(
              `column value expected to be array.`,
              TRANSFORM_NAME,
              header,
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
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
