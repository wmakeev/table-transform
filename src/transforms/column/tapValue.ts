import {
  TableChunksTransformer,
  TableRow,
  TransformStepColumnsNotFoundError
} from '../../index.js'

export interface TapColumnValueParams {
  column: string
  // TODO colIndex
  tapFunction: (value: unknown, row: TableRow, index: number) => void
}

const TRANSFORM_NAME = 'Column:TapValue'

// TODO Rename to simple tap
/**
 * Tap column value
 */
export const tapValue = (
  params: TapColumnValueParams
): TableChunksTransformer => {
  const { column, tapFunction } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const tapColumnHeader = tableHeader.find(
      h => !h.isDeleted && h.name === column
    )

    if (tapColumnHeader === undefined) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    return {
      ...source,
      getTableHeader: () => tableHeader,

      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          for (const row of chunk) {
            tapFunction(row[tapColumnHeader.index], row, tapColumnHeader.index)
          }
          yield chunk
        }
      }
    }
  }
}
