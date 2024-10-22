import assert from 'node:assert/strict'
import test from 'node:test'
import {
  generateColumnNumHeader,
  generateExcelStyleHeader,
  getChunkNormalizer,
  getExcelAddressCoordinates,
  getExcelHeaderColumnNum,
  getExcelOffset,
  getExcelRangeBound
} from '../../../src/tools/header/index.js'
import { ColumnHeader, TableRow } from '../../../src/types/index.js'

test('tools:header:getChunkNormalizer', async t => {
  await t.test('immutable=false | ordered', () => {
    const header: ColumnHeader[] = [
      {
        index: 0,
        isDeleted: false,
        name: 'col1'
      },
      {
        index: 1,
        isDeleted: false,
        name: 'col2'
      },
      {
        index: 2,
        isDeleted: false,
        name: 'col3'
      }
    ]

    const normalizer = getChunkNormalizer(header)

    const chunk: TableRow[] = [
      [0, 1, 2, 3, 4, 5],
      [6, 7, 8, 9]
    ]

    const result1 = normalizer(chunk)

    assert.equal(result1, chunk)
  })

  await t.test('immutable=true | ordered', () => {
    const header: ColumnHeader[] = [
      {
        index: 0,
        isDeleted: false,
        name: 'col1'
      },
      {
        index: 1,
        isDeleted: false,
        name: 'col2'
      },
      {
        index: 2,
        isDeleted: false,
        name: 'col3'
      }
    ]

    const normalizer = getChunkNormalizer(header, true)

    const chunk: TableRow[] = [
      [0, 1, 2, 3, 4, 5],
      [6, 7, 8, 9]
    ]

    const result1 = normalizer(chunk)

    assert.notEqual(result1, chunk)

    assert.deepEqual(result1, [
      [0, 1, 2],
      [6, 7, 8]
    ])
  })

  await t.test('immutable=true | unordered', () => {
    const header: ColumnHeader[] = [
      {
        index: 1,
        isDeleted: false,
        name: 'col2'
      },
      {
        index: 0,
        isDeleted: false,
        name: 'col1'
      },

      {
        index: 2,
        isDeleted: false,
        name: 'col3'
      }
    ]

    const normalizer = getChunkNormalizer(header)

    const chunk: TableRow[] = [
      [0, 1, 2, 3, 4, 5],
      [6, 7, 8, 9]
    ]

    const result1 = normalizer(chunk)

    assert.notEqual(result1, chunk)

    assert.deepEqual(result1, [
      [1, 0, 2],
      [7, 6, 8]
    ])
  })
})

test('tools:header:generateColumnNumHeader', () => {
  const header = generateColumnNumHeader(1000)

  assert.ok(header)
  assert.ok(header.length === 1000)

  assert.equal(header[0], 'Col1')
  assert.equal(header[10], 'Col11')
  assert.equal(header[999], 'Col1000')
})

test('tools:header:generateExcelStyleHeader', () => {
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

test('tools:header:getExcelHeaderColumnNum', () => {
  assert.equal(getExcelHeaderColumnNum('A'), 1)
  assert.equal(getExcelHeaderColumnNum('B'), 2)
  assert.equal(getExcelHeaderColumnNum('z'), 26)
  assert.equal(getExcelHeaderColumnNum('Aa'), 27)
  assert.equal(getExcelHeaderColumnNum('VD'), 576)
  assert.equal(getExcelHeaderColumnNum('ZZ'), 702)
  assert.equal(getExcelHeaderColumnNum('aaa'), 703)
  assert.equal(getExcelHeaderColumnNum('AIL'), 922)

  assert.throws(() => {
    getExcelHeaderColumnNum('A2')
  }, /Incorrect Excel header name/)

  assert.throws(() => {
    getExcelHeaderColumnNum('42')
  }, /Incorrect Excel header name/)

  assert.throws(() => {
    getExcelHeaderColumnNum('')
  }, /Empty Excel header name/)

  assert.throws(() => {
    getExcelHeaderColumnNum('A-')
  }, /Incorrect Excel header name/)

  assert.throws(() => {
    getExcelHeaderColumnNum('ABCD')
  }, /To long Excel header name/)
})

test('tools:header:getExcelAddressCoordinates', () => {
  assert.deepEqual(getExcelAddressCoordinates('A1'), { x: 0, y: 0 })
  assert.deepEqual(getExcelAddressCoordinates('D15'), { x: 3, y: 14 })
  assert.deepEqual(getExcelAddressCoordinates('d15'), { x: 3, y: 14 })

  assert.throws(() => {
    getExcelAddressCoordinates('A')
  }, /Incorrect Excel address/)

  assert.throws(() => {
    getExcelAddressCoordinates('Ad-15')
  }, /Incorrect Excel address/)

  assert.throws(() => {
    getExcelAddressCoordinates('15')
  }, /Incorrect Excel address/)
})

test('tools:header:getExcelRangeBound', () => {
  assert.deepEqual(
    getExcelRangeBound('A1'),
    { x1: 0, y1: 0, x2: 0, y2: 0 },
    'A1'
  )

  assert.deepEqual(
    getExcelRangeBound('C4'),
    { x1: 2, y1: 3, x2: 2, y2: 3 },
    'C4'
  )

  assert.deepEqual(
    getExcelRangeBound('A1:A1'),
    { x1: 0, y1: 0, x2: 0, y2: 0 },
    'A1:A1'
  )

  assert.deepEqual(
    getExcelRangeBound('B2:D5'),
    { x1: 1, y1: 1, x2: 3, y2: 4 },
    'B2:D5'
  )

  assert.throws(() => {
    getExcelRangeBound('A')
  }, /Incorrect Excel address/)

  assert.throws(() => {
    getExcelRangeBound('A1:')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound('A1:B')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound(':B')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound(':B2')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound('A1:B2ff')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound('23A1:B2')
  }, /Excel address/)

  assert.throws(() => {
    getExcelRangeBound('B3:A1')
  }, /Inverted Excel ranges not supported/)
})

test('tools:header:getExcelOffset', () => {
  assert.deepEqual(getExcelOffset(''), { x: 0, y: 0 }, 'empty string')
  assert.deepEqual(getExcelOffset('RC'), { x: 0, y: 0 }, 'RC')
  assert.deepEqual(getExcelOffset('R[1]C'), { x: 0, y: 1 }, 'R[1]C')
  assert.deepEqual(getExcelOffset('RC[1]'), { x: 1, y: 0 }, 'R[1]C')
  assert.deepEqual(getExcelOffset('R[1]C[1]'), { x: 1, y: 1 }, 'R[1]C[1]')
  assert.deepEqual(getExcelOffset('R[3]C[4]'), { x: 4, y: 3 }, 'R[3]C[4]')
  assert.deepEqual(getExcelOffset('R[-3]C[4]'), { x: 4, y: -3 }, 'R[-3]C[4]')
  assert.deepEqual(getExcelOffset('R[3]C[-4]'), { x: -4, y: 3 }, 'R[3]C[-4]')
  assert.deepEqual(getExcelOffset('R[-3]C[-4]'), { x: -4, y: -3 }, 'R[-3]C[-4]')

  assert.throws(() => {
    getExcelOffset('R')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('R[1]')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('C')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('C[2]')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('R1C2')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('R1C[2]')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('R[1]C[2]foo')
  }, /Incorrect offset value/)

  assert.throws(() => {
    getExcelOffset('12R[1]C[2]')
  }, /Incorrect offset value/)
})
