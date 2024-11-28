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
  createTableHeader,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { getFieldSumReducer } from '../../helpers/index.js'

test('transforms:column:probe*', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.probeTake({
        key: 'test_probe',
        column: 'probe_value'
      }),

      tf.reduceWith({
        reducer: getFieldSumReducer('number')
      }),

      tf.column.add({ column: 'probe' }),

      tf.column.probePut({
        key: 'test_probe',
        column: 'probe'
      }),

      tf.column.probeRestore({
        key: 'test_probe',
        column: 'probe_2'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'number', 'probe_value'],
      [1, 1, 'foo'],
      [2, 1, 'foo'],
      [3, 1, 'bar']
    ],
    [
      [4, 1, 'bar'],
      [5, 1, 'bar'],
      [6, 1, 'bar']
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['reduced', 'probe', 'probe_2'],
    [6, 'foo', 'foo']
  ])
})

test('transforms:column:probe*Many', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.probeTakeMany({
        columns: ['col1', 'col2', 'col3', 'col4']
      }),

      tf.reduceWith({
        reducer: getFieldSumReducer('val')
      }),

      tf.column.add({ column: 'col1' }),
      tf.column.add({ column: 'col3' }),

      tf.column.probePutMany({
        columns: ['col1', 'col3']
      }),

      tf.column.probeRestoreMany({
        columns: ['col2', 'col4']
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['col1', 'col2', 'col3', 'col4', 'val'],
      [11, 12, 13, 14, 1],
      [21, 22, 23, 24, 1],
      [31, 32, 33, 34, 1]
    ],
    [
      [41, 42, 43, 44, 1],
      [51, 52, 53, 54, 1]
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['reduced', 'col1', 'col3', 'col2', 'col4'],
    [5, 11, 13, 12, 14]
  ])
})

test('transforms:column:probe* - ahead generator', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.probeTake({
        column: 'probe_value'
      }),

      src => {
        return {
          ...src,
          getHeader: () => createTableHeader(['first']),
          async *[Symbol.asyncIterator]() {
            // Generate values before probe taken
            yield [[10], [20]]
            yield [[30], [40]]

            for await (const chunk of src) {
              yield [[chunk[0]![0]]]
            }
          }
        }
      },

      tf.column.add({ column: 'probe_value' }),

      tf.column.probePut({
        column: 'probe_value'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'number', 'probe_value'],
      [1, 1, 'foo'],
      [2, 1, 'foo'],
      [3, 1, 'bar']
    ],
    [
      [4, 1, 'bar'],
      [5, 1, 'bar'],
      [6, 1, 'bar']
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['first', 'probe_value'],
    [10, 'foo'],
    [20, 'foo'],
    [30, 'foo'],
    [40, 'foo'],
    [1, 'foo'],
    [4, 'foo']
  ])
})

// TODO Errors
