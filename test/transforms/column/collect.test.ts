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

test('transforms:column:collect', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.collect({
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

  assert.deepEqual(transformedRows, [[['value']], [[[6, 3, 5, 2, 4, 1]]]])
})

test('transforms:column:collect (resultColumn)', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.collect({
        column: 'value',
        resultColumn: 'collected'
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

  assert.deepEqual(transformedRows, [[['collected']], [[[6, 3, 5, 2, 4, 1]]]])
})
