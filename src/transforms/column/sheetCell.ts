import { TableChunksTransformer, TableRow } from '../../index.js'
import { getExcelOffset, getExcelRangeBound } from '../../tools/headers.js'

export type SheetCellParams = {
  /**
   * The operation through which the comparison with `testValue` is handled.
   *
   * default: `EQUAL`
   */
  operation?: SheetCellOperations

  /** Cell value to compare */
  testValue: unknown

  /**
   * The range in which to search for a cell.
   *
   * Excel style range like: `A1`, `A2:B10`
   */
  range: string
} & (
  | {
      /**
       * Transform mode
       *
       * - `ASSERT` - throws error if cell not found
       * - `CONSTANT` - use cell value as column value (usefull with offset)
       * - `HEADER` - interpret found cell as column header
       */
      type: 'ASSERT'
    }
  | {
      type: 'CONSTANT' | 'HEADER'

      /**
       * Collumn to place constant or column value
       */
      targetColumn: string

      /**
       * The offset to be shifted to target cell after the cell with `cellName`
       * is found in `range`.
       *
       * Excel RC style offset like: `R1`, `C2`, `R1C1`
       */
      offset?: string

      /**
       * If `true` not error was thrown if column not found
       */
      isOptional?: boolean
    }
)

interface FoundCell {
  rowIndex: number
  columnIndex: number
  value: any
}

export type SheetCellOperations =
  | 'STARTS_WITH'
  | 'EQUAL'
  | 'INCLUDES'
  | 'TEMPLATE'

const operations: Record<
  SheetCellOperations,
  (actual: unknown, expected: unknown) => boolean
> = {
  STARTS_WITH: (actual, expected) => {
    return actual != null && expected != null
      ? String(expected).trim().startsWith(String(actual).trim())
      : false
  },

  EQUAL: (actual, expected) => {
    return actual == null && expected == null ? true : expected === actual
  },

  INCLUDES: (actual, expected) => {
    return actual == null || expected == null
      ? false
      : String(expected).includes(String(actual))
  },

  TEMPLATE: () => {
    throw new Error('Not implemented')
  }
}

/**
 * Dynamic column
 */
export const sheetCell = (params: SheetCellParams): TableChunksTransformer => {
  const { type, operation = 'EQUAL', testValue, range } = params

  const { x1, y1, x2, y2 } = getExcelRangeBound(range)
  const { x: xOffset, y: yOffset } = getExcelOffset(
    type !== 'ASSERT' ? params.offset ?? '' : ''
  )

  const compareFn = operations[operation]

  return async ({ header, getSourceGenerator }) => {
    let rowBuffer: TableRow[] = []

    /** Pass chunks as is without modifications  */
    let isPassThrough = false

    let foundCell: FoundCell | null = null

    const targetColHeader =
      type !== 'ASSERT'
        ? header.find(h => !h.isDeleted && h.name === params.targetColumn) ??
          null
        : null

    if (type !== 'ASSERT' && targetColHeader === null) {
      throw new Error(`Column "${params.targetColumn}" not found`)
    }

    const [searchFrameFirstRowIndex, , , searchFrameLastRowIndex] = [
      y1,
      y1 + yOffset,
      y2,
      y2 + yOffset
    ].sort() as [number, number, number, number]

    if (searchFrameFirstRowIndex < 0) {
      throw new Error('Rows offset out of range')
    }

    const [searchFrameFirstColIndex, , , searchFrameLastColIndex] = [
      x1,
      x1 + xOffset,
      x2,
      x2 + xOffset
    ].sort() as [number, number, number, number]

    if (
      searchFrameFirstColIndex < 0 ||
      searchFrameLastColIndex > header.length
    ) {
      throw new Error('Columns offset out of range')
    }

    const searchCell = (bufferRowIndex: number): FoundCell | null => {
      let cellColIndex = -1

      const bufferRow = rowBuffer[bufferRowIndex]!

      for (
        let colIndex = searchFrameFirstColIndex;
        colIndex <= searchFrameLastColIndex;
        colIndex++
      ) {
        if (compareFn(bufferRow[colIndex], testValue)) {
          cellColIndex = colIndex
          break
        }
      }

      if (cellColIndex === -1) return null

      const rowIndex = bufferRowIndex + yOffset
      const columnIndex = cellColIndex + xOffset

      const value = rowBuffer[rowIndex]![columnIndex]

      return { rowIndex, columnIndex, value }
    }

    const processChunk = (chunk: TableRow[], fromRow = 0) => {
      if (targetColHeader === null || foundCell === null) return chunk

      for (let i = fromRow; i < chunk.length; i++) {
        chunk[i]![targetColHeader.index] =
          type === 'CONSTANT'
            ? foundCell.value
            : chunk[i]![foundCell.columnIndex]
      }

      return chunk
    }

    async function* getTransformedSourceGenerator() {
      let bufferFirstRowIndex: number | null = null

      let curRowIndex = 0

      chunkLoop: for await (const chunk of getSourceGenerator()) {
        if (isPassThrough) {
          yield chunk
          continue
        }

        if (foundCell !== null) {
          yield processChunk(chunk)
          continue
        }

        // Pass all chunks before frame start
        if (curRowIndex + chunk.length < searchFrameFirstRowIndex) {
          curRowIndex += chunk.length
          yield chunk
          continue
        }

        rowBuffer.push(...chunk)

        // Frame start inside buffer

        if (bufferFirstRowIndex === null) bufferFirstRowIndex = curRowIndex

        const bufferRowFrameEndRowIndex =
          bufferFirstRowIndex + rowBuffer.length - 1

        if (bufferRowFrameEndRowIndex < searchFrameLastRowIndex) continue

        // Frame end inside buffer

        for (let i = y1 - bufferFirstRowIndex; i <= y2; i++) {
          const found = searchCell(i)

          if (found !== null) {
            foundCell = found

            yield processChunk(
              rowBuffer,
              found.rowIndex + 1 - bufferFirstRowIndex
            )

            rowBuffer = []

            continue chunkLoop
          }
        }

        if (type !== 'ASSERT' && params.isOptional) {
          yield rowBuffer
          isPassThrough = true
          rowBuffer = []
          continue
        }

        throw new Error(`Cell "${testValue}" in "${range}" range not found`)
      }
    }

    return {
      header,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
