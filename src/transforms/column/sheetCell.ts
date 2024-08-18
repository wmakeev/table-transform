import assert from 'node:assert'
import {
  TransformChunkError,
  TransformRowError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'
import { getExcelOffset, getExcelRangeBound } from '../../tools/header/index.js'

const TRANSFORM_NAME = 'Column:SheetCell'

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
       * Array index, if `targetColumn` is array column
       */
      targetColumnIndex?: number

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
      targetColumnIndex?: number
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
  bufferRowIndex: number
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
    throw new TransformStepError('Not implemented', TRANSFORM_NAME)
  },

  EMPTY: (str1, str2) => {
    if (str2 != null || (typeof str2 === 'string' && str2 !== '')) {
      throw new TransformStepError(
        'EMPTY function should receive empty second argument',
        TRANSFORM_NAME
      )
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
    type !== 'ASSERT' ? (params.offset ?? '') : ''
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
          ? (srcHeader.filter(
              h => !h.isDeleted && h.name === params.targetColumn
            )?.[params.targetColumnIndex ?? 0] ?? null)
          : null

      if (type !== 'ASSERT' && targetColHeader === null) {
        throw new TransformStepError(
          `Column "${params.targetColumn}" not found`,
          TRANSFORM_NAME
        )
      }

      const [searchFrameFirstRowIndex, , , searchFrameLastRowIndex] = [
        y1,
        y1 + yOffset,
        y2,
        y2 + yOffset
      ].sort() as [number, number, number, number]

      if (searchFrameFirstRowIndex < 0) {
        throw new TransformStepError('Rows offset out of range', TRANSFORM_NAME)
      }

      /* eslint prefer-const:0 */
      let [searchFrameFirstColIndex, , , searchFrameLastColIndex] = [
        x1,
        x1 + xOffset,
        x2,
        x2 + xOffset
      ].sort() as [number, number, number, number]

      if (searchFrameFirstColIndex < 0) {
        throw new TransformStepError(
          'Columns offset out of range',
          TRANSFORM_NAME
        )
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
          try {
            if (compareFn(bufferRow[colIndex], testValue)) {
              cellColIndex = colIndex
              break
            }
          } catch (err) {
            assert.ok(err instanceof Error)

            throw new TransformRowError(
              err.message,
              TRANSFORM_NAME,
              srcHeader,
              rowBuffer,
              bufferRowIndex,
              colIndex,
              { cause: err }
            )
          }
        }

        if (cellColIndex === -1) return null

        const foundRowIndex = bufferRowIndex + yOffset
        const columnIndex = cellColIndex + xOffset

        const value = rowBuffer[foundRowIndex]![columnIndex]

        return { bufferRowIndex: foundRowIndex, columnIndex, value }
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

        for (
          let i = y1 - bufferFirstRowIndex, len = y2 - bufferFirstRowIndex;
          i <= len;
          i++
        ) {
          const found = searchCell(i)

          if (found !== null) {
            foundCell = found

            yield processChunk(rowBuffer, found.bufferRowIndex + 1)

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

        throw new TransformChunkError(
          `Cell "${testValue}" in "${range}" range not found`,
          TRANSFORM_NAME,
          srcHeader,
          rowBuffer
        )
      }
    }

    return {
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
