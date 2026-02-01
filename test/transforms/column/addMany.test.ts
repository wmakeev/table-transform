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

test('transforms:column:addMany', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.addMany({
        columns: ['b', 'c', 'd'],
        defaultValue: 'new'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [[['a'], [1], [2], [3]]]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows[0], [
    [['a', 'b', 'c', 'd']],
    [
      [1, 'new', 'new', 'new'],
      [2, 'new', 'new', 'new'],
      [3, 'new', 'new', 'new']
    ]
  ])
})
