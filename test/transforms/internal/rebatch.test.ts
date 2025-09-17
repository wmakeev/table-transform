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
  TransformStepParameterError,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:internal:rebatch (wrong parameters error)', async () => {
  try {
    tf.internal.rebatch({ count: 0 })
    assert.fail('Error expected')
  } catch (err) {
    assert.ok(err instanceof TransformStepParameterError)
  }
})

test('transforms:internal:rebatch', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.internal.rebatch({
        count: 4
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [['row_num'], [1], [2], [3]],
    [[4], [5], [6]],
    [[7], [8]],
    [[9], [10], [11], [12]],
    [[13], [14]]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    [['row_num']],
    [[1], [2], [3], [4]],
    [[5], [6], [7], [8]],
    [[9], [10], [11], [12]],
    [[13], [14]]
  ])
})

test('transforms:internal:rebatch (with count 1)', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.internal.rebatch({
        count: 1
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [['row_num'], [1], [2], [3]],
    [[4], [5], [6]],
    [[7], [8]],
    [[9], [10], [11], [12]],
    [[13], [14]]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    [['row_num']],
    [[1]],
    [[2]],
    [[3]],
    [[4]],
    [[5]],
    [[6]],
    [[7]],
    [[8]],
    [[9]],
    [[10]],
    [[11]],
    [[12]],
    [[13]],
    [[14]]
  ])
})
