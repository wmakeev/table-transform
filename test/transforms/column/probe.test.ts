import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  FlattenTransform,
  TableChunksReducer,
  TableRow,
  TableTransfromConfig,
  createTableHeader,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

const getFieldSumReducer: (column: string) => TableChunksReducer =
  column => src => {
    const sumColHeader = src
      .getHeader()
      .find(h => h.isDeleted === false && h.name === column)

    if (sumColHeader === undefined) {
      throw new Error(`Column "${column}" not found`)
    }

    return {
      outputColumns: ['reduced'],
      async getResult() {
        let reducedSum = 0

        for await (const chunk of src) {
          reducedSum = chunk.reduce((res, row) => {
            const num = row[sumColHeader.index]

            return typeof num === 'number' ? res + num : res
          }, reducedSum)
        }

        return [reducedSum]
      }
    }
  }

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
    ['reduced', 'probe'],
    [6, 'foo']
  ])
})

test('transforms:column:probe* - ahead generator', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.probeTake({
        key: 'test_probe',
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

      tf.column.add({ column: 'probe' }),

      tf.column.probePut({
        key: 'test_probe',
        column: 'probe'
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
    ['first', 'probe'],
    [10, 'foo'],
    [20, 'foo'],
    [30, 'foo'],
    [40, 'foo'],
    [1, 'foo'],
    [4, 'foo']
  ])
})
