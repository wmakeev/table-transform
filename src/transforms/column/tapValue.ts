import {
  TableChunksTransformer,
  TableRow,
  TransformColumnsNotFoundError
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
    const header = source.getHeader()

    const tapColumnHeader = header.find(h => !h.isDeleted && h.name === column)

    if (tapColumnHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    return {
      ...source,
      getHeader: () => header,

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
