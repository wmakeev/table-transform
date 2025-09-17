import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  FlattenTransform,
  TableRow,
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:column:tapValue', async () => {
  const values: unknown[] = []

  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.tapValue({
        column: 'val',
        tapFunction: (val, row, index) => {
          values.push(val)
          assert.equal(val, row[index])
        }
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'val'],
      [1, 6],
      [2, 3],
      [3, 5]
    ],
    [
      [4, 2],
      [5, 4],
      [6, 1]
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['row_num', 'val'],
    [1, 6],
    [2, 3],
    [3, 5],
    [4, 2],
    [5, 4],
    [6, 1]
  ])

  assert.deepEqual(values, [6, 3, 5, 2, 4, 1])
})
