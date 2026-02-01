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

test('transforms:column:renameMany', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.renameMany({
        renames: {
          a: 'foo_a',
          c: 'foo_c',
          e: 'foo_e'
        }
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [
    [
      ['a', 'b', 'c', 'e'],
      [1, 6],
      [2, 3],
      [3, 5]
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows[0], [['foo_a', 'b', 'foo_c', 'foo_e']])
})
