import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  TableRow,
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:column:rollup', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.rollup({
        columns: ['value']
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

test('transforms:column:rollup (many)', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.rollup({
        columns: ['value', 'row_num']
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
    [['value', 'row_num']],
    [
      [
        [6, 3, 5, 2, 4, 1],
        [1, 2, 3, 4, 5, 6]
      ]
    ]
  ])
})

test('transforms:column:rollup (empty)', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.rollup({
        columns: ['value']
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [[['row_num', 'value']]]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [[['value']]])
})
