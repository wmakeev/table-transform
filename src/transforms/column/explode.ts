import {
  TransformColumnsNotFoundError,
  TransformHeaderError
} from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer, TableRow } from '../../index.js'

const TRANSFORM_NAME = 'Column:Explode'

export interface ExplodeColumnParams {
  column: string
  arrIndex?: number
}

/**
 * Explode row
 */
export const explode = (
  params: ExplodeColumnParams
): TableChunksTransformer => {
  const { column, arrIndex = 0 } = params

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
      for await (const chunk of source) {
        const explodedRows: TableRow[] = []

        for (const row of chunk) {
          const explodingCellValue = row[explodeColumn!.index]

          if (Array.isArray(explodingCellValue)) {
            for (const item of explodingCellValue) {
              const newRow = [...row]
              newRow[explodeColumn!.index] = item
              explodedRows.push(newRow)
            }
          } else {
            explodedRows.push(row)
          }
        }

        yield explodedRows
      }
    }

    return {
      ...source,
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
