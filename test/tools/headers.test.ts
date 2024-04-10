import assert from 'node:assert/strict'
import test from 'node:test'
import {
  generateColumnNumHeader,
  generateExcelStyleHeader
} from '../../src/tools/headers.js'

test('generateColumnNumHeader', () => {
  const header = generateColumnNumHeader(1000)

  assert.ok(header)
  assert.ok(header.length === 1000)

  assert.equal(header[0], 'Col1')
  assert.equal(header[10], 'Col11')
  assert.equal(header[999], 'Col1000')
})

test('generateExcelStyleHeader', () => {
  const header = generateExcelStyleHeader(1000)

  assert.ok(header)
  assert.ok(header.length === 1000)

  assert.equal(header[0], 'A')
  assert.equal(header[10], 'K')
  assert.equal(header[25], 'Z')
  assert.equal(header[26], 'AA')
  assert.equal(header[27], 'AB')
  assert.equal(header[55], 'BD')
  assert.equal(header[258], 'IY')
  assert.equal(header[701], 'ZZ')
  assert.equal(header[702], 'AAA')
  assert.equal(header[865], 'AGH')
})
