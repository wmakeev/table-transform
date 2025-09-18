import assert from 'node:assert'
import test from 'node:test'
import {
  createRecordFromRow,
  forceArrayLength,
  TableHeader,
  TableRow
} from '../../src/index.js'

test('Tools:getRowRecord', () => {
  const header: TableHeader = [
    {
      name: 'Foo',
      index: 0,
      isDeleted: true
    },
    {
      name: 'Foo',
      index: 2,
      isDeleted: false
    },
    {
      name: 'Bar',
      index: 1,
      isDeleted: false
    },
    {
      name: 'Foo',
      index: 4,
      isDeleted: false
    },
    {
      name: 'Baz',
      index: 3,
      isDeleted: false
    }
  ]

  const row: TableRow = ['foo0', 'bar1', 'foo2', 'baz3', 'foo4']

  const result = createRecordFromRow(header, row)

  assert.deepEqual(result, {
    Foo: ['foo2', 'foo4'],
    Bar: 'bar1',
    Baz: 'baz3'
  })
})

test('Tools:forceArrayLength', () => {
  const arr = [1, 2, 3, 4, 5, 6, 7, 8, 9]

  forceArrayLength(arr, 5)

  assert.deepEqual(arr, [1, 2, 3, 4, 5])

  forceArrayLength(arr, 9)

  assert.deepEqual(arr, [1, 2, 3, 4, 5, null, null, null, null])

  forceArrayLength(arr, 9)

  assert.deepEqual(arr, [1, 2, 3, 4, 5, null, null, null, null])
})
