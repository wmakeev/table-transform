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
  TransformStepParameterError,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { FlattenTransform } from '../../src/index.js'

test('transforms:take #0', async () => {
  try {
    tf.take({ count: 0 })
    assert.fail('Error expected')
  } catch (err) {
    assert.ok(err instanceof TransformStepParameterError)
  }
})

test('transforms:take #1', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.take({
        count: 2
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
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['row_num', 'value'],
    [1, 6],
    [2, 3]
  ])
})

test('transforms:take #2', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.take({
        count: 3
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
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['row_num', 'value'],
    [1, 6],
    [2, 3],
    [3, 5]
  ])
})

test('transforms:take #3', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.take({
        count: 4
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
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['row_num', 'value'],
    [1, 6],
    [2, 3],
    [3, 5],
    [4, 2]
  ])
})
