import assert from 'node:assert'
import { TransformBugError } from '../../errors/index.js'
import { ColumnHeader, HeaderMode, TableRow } from '../../index.js'
import { generateExcelStyleHeader } from './excel.js'

export * from './excel.js'

/**
 * @returns `true` if no columns is deleted or reordered
 */
export function isHeaderNormalized(header: ColumnHeader[]): boolean {
  for (let i = 0; i < header.length; i++) {
    const col = header[i]
    if (col!.isDeleted || col!.index !== i) return false
  }

  return true
}

/**
 * @param immutable
 * @returns remove deleted columns and reorder
 */
export function getChunkNormalizer(header: ColumnHeader[], immutable = false) {
  const actualHeader = header.filter(h => !h.isDeleted)

  // Optimization if no columns is deleted or reordered
  if (!immutable && isHeaderNormalized(header)) {
    return (rowsChunk: TableRow[]) => rowsChunk
  }

  return function normalizeRowsChunk(rowsChunk: TableRow[]) {
    const normalizedRowsChunk: TableRow[] = []

    for (const row of rowsChunk) {
      const resultRow: TableRow = []

      for (const h of actualHeader) {
        resultRow.push(row[h.index])
      }

      normalizedRowsChunk.push(resultRow)
    }

    return normalizedRowsChunk
  }
}

export const createTableHeader = (columnsNames: TableRow) => {
  const header: ColumnHeader[] = columnsNames.map((h, index) => {
    const colMeta: ColumnHeader = {
      index: index,
      name: String(h),
      isDeleted: false
    }

    return colMeta
  })

  return header
}

/**
 * Return non deleted header columns names
 *
 * @param header Header
 * @returns Normalized columns
 */
export const getNormalizedHeaderRow = (header: ColumnHeader[]): string[] => {
  return header.filter(h => !h.isDeleted).map(h => h.name)
}

export function generateColumnNumHeader(colsCount: number) {
  assert.ok(colsCount <= 1000, 'Expected to be less then 1000 columns')
  assert.ok(colsCount > 0, 'Expected to be not 0 columns')

  const header: string[] = Array(colsCount)

  for (let i = 0; i < colsCount; i++) header[i] = `Col${i + 1}`

  return header
}

export const generateHeaderColumnNames = (
  headerMode: HeaderMode,
  count: number
): string[] => {
  let columnsNames: string[]

  switch (headerMode) {
    case 'COLUMN_NUM': {
      columnsNames = generateColumnNumHeader(count)
      break
    }
    case 'EXCEL_STYLE': {
      columnsNames = generateExcelStyleHeader(count)
      break
    }

    default: {
      throw new TransformBugError(
        `generateHeader: Unsupported header mode - ${headerMode}`
      )
    }
  }

  return columnsNames
}
