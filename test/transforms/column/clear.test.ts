import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:column:clear', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.clear({
        column: 'value'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'value'],
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
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    [['row_num', 'value']],
    [
      [1, null],
      [2, null],
      [3, null]
    ],
    [
      [4, null],
      [5, null],
      [6, null]
    ]
  ])
})
