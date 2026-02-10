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
import { createTestContext } from '../../_common/TestContext.js'

test('transforms:column:transformAndRename', async () => {
  const tableTransformConfig: TableTransformConfig = {
    context: createTestContext(),
    transforms: [
      tf.column.transformAndRename({
        name: 'Transform and rename',
        column: 'a',
        expression: `value() + 1`,
        newName: 'b'
      })
    ]
  }

  const sourceDataChunks: TableRow[][] = [[['a'], [1], [2]]]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig)
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [[['b']], [[2], [3]]])
})
