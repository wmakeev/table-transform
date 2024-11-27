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

test('transforms:column:wait', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.add({
        column: 'timeout',
        defaultValue: 300
      }),

      tf.column.wait({
        timeoutColumn: 'timeout'
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
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    [['row_num', 'val', 'timeout']],
    [[1, 6, 300]],
    [[2, 3, 300]],
    [[3, 5, 300]],
    [[4, 2, 300]],
    [[5, 4, 300]],
    [[6, 1, 300]]
  ])
})
