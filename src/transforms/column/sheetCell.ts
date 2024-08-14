import { TransformAssertError } from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'
import { getExcelOffset, getExcelRangeBound } from '../../tools/header/index.js'

export type SheetCellParams =
  | {
      /**
       * Transform mode
       *
       * - `ASSERT` - throws error if cell not found
       * - `CONSTANT` - use cell value as column value (usefull with offset)
       * - `HEADER` - interpret found cell as column header
       */
      type: 'CONSTANT' | 'HEADER'

      /**
       * The range in which to search for a cell.
       *
       * Excel style range like: `A1`, `A2:B10`
       */
      range: string

      testOperation?: Exclude<SheetCellOperations, 'NOOP' | 'EMPTY'> | undefined

      /** Cell value to compare */
      testValue?: unknown

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
  | {
      type: 'CONSTANT' | 'HEADER'
      range: string
      testOperation: Extract<SheetCellOperations, 'NOOP' | 'EMPTY'>
      testValue: undefined
      targetColumn: string
      offset?: string
      isOptional?: boolean
    }
  | {
      type: 'ASSERT'
      range: string
      testOperation: Extract<SheetCellOperations, 'EMPTY'>
      testValue?: undefined
    }
  | {
      type: 'ASSERT'
      range: string
      testOperation?: Exclude<SheetCellOperations, 'NOOP' | 'EMPTY'> | undefined
      testValue: unknown
    }

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
  | 'EMPTY'
  | 'NOOP'

const operations: Record<
  SheetCellOperations,
  (str1: unknown, str2: unknown) => boolean
> = {
  STARTS_WITH: (str1, str2) => {
    return str1 != null && str2 != null
      ? String(str1).trim().startsWith(String(str2).trim())
      : false
  },

  EQUAL: (str1, str2) => {
    return str1 == null && str2 == null ? true : str2 === str1
  },

  INCLUDES: (str1, str2) => {
    return str1 == null || str2 == null
      ? false
      : String(str1).includes(String(str2))
  },

  TEMPLATE: () => {
    throw new Error('Not implemented')
  },

  EMPTY: (str1, str2) => {
    if (str2 != null || (typeof str2 === 'string' && str2 !== '')) {
      throw new Error('EMPTY function should receive empty second argument')
    }
    return str1 == null || str1 === ''
  },

  NOOP: () => true
}

/**
 * Dynamic column
 */
export const sheetCell = (params: SheetCellParams): TableChunksTransformer => {
  const { type, testOperation = 'EQUAL', testValue, range } = params

  const { x1, y1, x2, y2 } = getExcelRangeBound(range)
  const { x: xOffset, y: yOffset } = getExcelOffset(
    type !== 'ASSERT' ? params.offset ?? '' : ''
  )

  const compareFn = operations[testOperation]

  return source => {
    async function* getTransformedSourceGenerator() {
      const srcHeader = source.getHeader()

      let rowBuffer: TableRow[] = []

      /** Pass chunks as is without modifications  */
      let isPassThrough = false

      let foundCell: FoundCell | null = null

      const targetColHeader =
        type !== 'ASSERT'
          ? srcHeader.find(
              h => !h.isDeleted && h.name === params.targetColumn
            ) ?? null
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

      /* eslint prefer-const:0 */
      let [searchFrameFirstColIndex, , , searchFrameLastColIndex] = [
        x1,
        x1 + xOffset,
        x2,
        x2 + xOffset
      ].sort() as [number, number, number, number]

      if (searchFrameFirstColIndex < 0) {
        throw new Error('Columns offset out of range')
      }

      if (searchFrameLastColIndex >= srcHeader.length) {
        searchFrameLastColIndex = srcHeader.length - 1
      }

      const searchCell = (bufferRowIndex: number): FoundCell | null => {
        let cellColIndex = -1

        const bufferRow = rowBuffer[bufferRowIndex]!

        for (
          let colIndex = searchFrameFirstColIndex;
          colIndex <= searchFrameLastColIndex;
          colIndex++
        ) {
          // Search only in headers from source data, and skip added headers
          if (srcHeader[colIndex]?.isFromSource !== true) continue

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

      let bufferFirstRowIndex: number | null = null

      let curRowIndex = 0

      chunkLoop: for await (const chunk of source) {
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

        if (type !== 'ASSERT' && params.isOptional === true) {
          yield rowBuffer
          isPassThrough = true
          rowBuffer = []
          continue
        }

        throw new TransformAssertError(
          `Cell "${testValue}" in "${range}" range not found`,
          'SheetCell'
        )
      }
    }

    return {
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
