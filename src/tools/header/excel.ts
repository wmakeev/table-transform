import assert from 'node:assert/strict'
import {
  CHAR_COUNT,
  CHAR_FROM,
  EXCEL_ADDRESS_REGEX,
  EXCEL_RC_OFFSET_REGEX
} from './constants.js'

/**
 * @throws {Error}
 */
function* excelHeaderGen(): Generator<string, any, undefined> {
  let init = -1

  for (let p3 = init; p3 < CHAR_COUNT; p3++) {
    for (let p2 = init; p2 < CHAR_COUNT; p2++) {
      for (let p1 = 0; p1 < CHAR_COUNT; p1++) {
        yield [
          p3 === -1 ? '' : String.fromCharCode(CHAR_FROM + p3),
          p2 === -1 ? '' : String.fromCharCode(CHAR_FROM + p2),
          String.fromCharCode(CHAR_FROM + p1)
        ].join('')
      }
    }
    init = 0
  }
  throw new Error('Too many headers')
}

export function generateExcelStyleHeader(colsCount: number) {
  assert.ok(colsCount <= 1000, 'Expected to be less then 1000 columns')
  assert.ok(colsCount > 0, 'Expected to be not 0 columns')

  const header: string[] = []

  let rest = colsCount
  for (const h of excelHeaderGen()) {
    header.push(h)
    if (rest-- === 1) break
  }

  return header
}

/**
 * @param excelHeaderName Name of Excel column like `AB`
 * @throws {Error}
 */
export function getExcelHeaderColumnNum(excelHeaderName: string) {
  const symbolsNumsReversed = excelHeaderName
    .toUpperCase()
    .split('')
    .map(sym => sym.charCodeAt(0) - CHAR_FROM + 1)
    .reverse()

  if (symbolsNumsReversed.length === 0) {
    throw new Error('Empty Excel header name')
  }

  if (symbolsNumsReversed.some(n => n < 1 || n > 26)) {
    throw new Error(`Incorrect Excel header name - ${excelHeaderName}`)
  }

  if (symbolsNumsReversed.length > 3) {
    throw new Error(`To long Excel header name - ${excelHeaderName}`)
  }

  const colNum = symbolsNumsReversed
    .map((num, index) => {
      return num * Math.pow(CHAR_COUNT, index)
    })
    .reduce((res, it) => res + it)

  return colNum
}

/**
 * @param address Excel style addres
 * @throws {Error}
 */
export function getExcelAddressCoordinates(address: string): Coordinates {
  if (typeof address !== 'string' || address.length === 0) {
    throw new Error('Excel address should not to be empty string')
  }

  const match = EXCEL_ADDRESS_REGEX.exec(address)

  if (match === null) {
    throw new Error(`Incorrect Excel address - ${address}`)
  }

  if (match?.length !== 3) {
    throw new Error(`Incorrect Excel address - ${address}`)
  }

  const column = getExcelHeaderColumnNum(match[1]!)
  const row = Number.parseInt(match[2]!)

  return {
    x: column - 1,
    y: row - 1
  }
}

export interface Coordinates {
  x: number
  y: number
}

export interface ExcelRangeBound {
  x1: number
  y1: number
  x2: number
  y2: number
}

export interface Offset {
  x: number
  y: number
}

/**
 * @throws {Error}
 */
export function getExcelRangeBound(range: string): ExcelRangeBound {
  const [cellFrom, cellTo] = range.split(':') as [string] | [string, string]

  const cellFromCoord = getExcelAddressCoordinates(cellFrom)

  let cellToCoord: Coordinates

  if (cellTo == null) {
    cellToCoord = cellFromCoord
  } else {
    cellToCoord = getExcelAddressCoordinates(cellTo)
  }

  const result: ExcelRangeBound = {
    x1: cellFromCoord.x,
    y1: cellFromCoord.y,
    x2: cellToCoord.x,
    y2: cellToCoord.y
  }

  if (result.x1 > result.x2 || result.y1 > result.y2) {
    throw new Error(`Inverted Excel ranges not supported - ${range}`)
  }

  return result
}

/**
 * @throws {Error}
 */
export function getExcelOffset(offset: string): Offset {
  if (typeof offset !== 'string') {
    throw new Error('Expected offset to be string value')
  }

  if (offset === '') {
    return { x: 0, y: 0 }
  }

  const match = EXCEL_RC_OFFSET_REGEX.exec(offset)

  if (match === null) {
    throw new Error(`Incorrect offset value - ${offset}`)
  }

  if (match.length !== 3) {
    throw new Error(`Incorrect offset value - ${offset}`)
  }

  // "RC"
  if (match[1] == null && match[2] == null) {
    return { x: 0, y: 0 }
  }

  // "R[1]C"
  else if (match[2] == null) {
    return {
      x: 0,
      y: Number.parseInt(match[1]!)
    }
  }

  // "R[1]C[2]" | "RC[2]"
  else {
    return {
      x: match[2] ? Number.parseInt(match[2]!) : 0,
      y: match[1] == null ? 0 : Number.parseInt(match[1])
    }
  }
}
