import assert from 'node:assert'
import { TransformBugError } from '../../errors/index.js'
import { ColumnHeader, HeaderMode, TableHeader, TableRow } from '../../index.js'
import { generateExcelStyleHeader } from './excel.js'

export * from './excel.js'

/**
 * Find column header by name
 */
export function findActiveHeader(headerName: string, tableHeader: TableHeader) {
  return (
    tableHeader.find(h => h.isDeleted === false && h.name === headerName) ??
    null
  )
}

/**
 * Returns not deleted headers by it names
 */
export function findActiveHeaders(headerNames: string[], header: TableHeader) {
  return headerNames.map(name => findActiveHeader(name, header))
}

/**
 * @returns `true` if no columns is deleted or reordered
 */
export function isHeaderNormalized(header: TableHeader): boolean {
  for (let i = 0; i < header.length; i++) {
    const col = header[i]
    if (col!.isDeleted || col!.index !== i) return false
  }

  return true
}

/**
 * @param immutable
 */
export function getChunkNormalizer(header: TableHeader, immutable = false) {
  // Optimization if no columns is deleted or reordered
  if (!immutable && isHeaderNormalized(header)) {
    return {
      normalizedHeader: header,
      chunkNormalizer: (rowsChunk: TableRow[]) => rowsChunk
    }
  }

  const actualHeader = header.filter(h => !h.isDeleted)

  return {
    normalizedHeader: actualHeader.map((h, index) => ({
      ...h,
      index
    })),

    chunkNormalizer: function normalizeRowsChunk(rowsChunk: TableRow[]) {
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
}

export const createTableHeader = (columnsNames: TableRow) => {
  const header: TableHeader = columnsNames.map((h, index) => {
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
export const getNormalizedHeaderRow = (header: TableHeader): string[] => {
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
