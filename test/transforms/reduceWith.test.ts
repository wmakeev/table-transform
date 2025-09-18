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
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'

test('transforms:reduceWith', async () => {
  const getFieldSumReducer: (column: string) => TableChunksReducer =
    column => src => {
      const sumColHeader = src
        .getTableHeader()
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

  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.reduceWith({
        reducer: getFieldSumReducer('number')
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['row_num', 'number'],
      [1, 1],
      [2, 1],
      [3, 1]
    ],
    [
      [4, 1],
      [5, 1],
      [6, 1]
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [['reduced'], [6]])
})
