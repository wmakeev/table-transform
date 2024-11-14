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
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:column:sort (asc)', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.sort({
        column: 'order'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'order'],
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
    ['row_num', 'order'],
    [6, 1],
    [4, 2],
    [2, 3],
    [5, 4],
    [3, 5],
    [1, 6]
  ])
})

test('transforms:column:sort (desc)', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.sort({
        column: 'order',
        order: 'desc'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'order'],
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
    ['row_num', 'order'],
    [1, 6],
    [3, 5],
    [5, 4],
    [2, 3],
    [4, 2],
    [6, 1]
  ])
})
