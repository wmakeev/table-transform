import assert from 'node:assert'
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
} from '../../src/index.js'
import { getFieldSumReducer } from '../helpers/index.js'

test('transforms:rowsBuffer - maxBufferSize=0', async () => {
  const chunksTimeline: TableRow[][] = []

  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.tapRows({
        tapFunction(chunk) {
          chunksTimeline.push(chunk)
        }
      }),

      tf.rowsBuffer({ maxBufferSize: 0 }),

      tf.tapRows({
        tapFunction(chunk) {
          chunksTimeline.push(chunk)
        }
      }),

      tf.reduceWith({
        reducer: getFieldSumReducer('row_num')
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [['row_num']],
    [[1], [2]],
    [[3], [4]],
    [[5], [6], [7], [8], [9]]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(chunksTimeline, [
    [[1], [2]],
    [[1]],
    [[2]],
    [[3], [4]],
    [[3]],
    [[4]],
    [[5], [6], [7], [8], [9]],
    [[5]],
    [[6]],
    [[7]],
    [[8]],
    [[9]]
  ])

  assert.deepEqual(transformedRows, [[['reduced']], [[45]]])
})

test('transforms:rowsBuffer - maxBufferSize=3', async () => {
  const chunksTimeline: TableRow[][] = []

  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.tapRows({
        tapFunction(chunk) {
          chunksTimeline.push(chunk)
        }
      }),

      tf.rowsBuffer({ maxBufferSize: 3 }),

      tf.tapRows({
        tapFunction(chunk) {
          chunksTimeline.push(chunk)
        }
      }),

      tf.reduceWith({
        reducer: getFieldSumReducer('row_num')
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [['row_num']],
    [[1], [2]],
    [[3], [4]],
    [[5], [6], [7]],
    [[8], [9]]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(chunksTimeline, [
    [[1], [2]],
    [[1]],
    [[3], [4]],
    [[5], [6], [7]],
    [[2]],
    [[3]],
    [[4]],
    [[8], [9]],
    [[5]],
    [[6]],
    [[7]],
    [[8]],
    [[9]]
  ])

  assert.deepEqual(transformedRows, [[['reduced']], [[45]]])
})
