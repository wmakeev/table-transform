import assert from 'node:assert'

export function generateColumnNumHeader(colsCount: number) {
  assert.ok(colsCount <= 1000, 'Expected to be less then 1000 columns')
  assert.ok(colsCount > 0, 'Expected to be not 0 columns')

  const header: string[] = Array(colsCount)

  for (let i = 0; i < colsCount; i++) header[i] = `Col${i + 1}`

  return header
}

const CHAR_FROM = 65 // A
const CHAR_COUNT = 26 // A-Z

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
