import {
  // @ts-expect-error no typings for compose
  compose,
  Readable
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  createTableTransformer,
  TransformStepColumnsNotFoundError,
  transforms as tf
} from '../../../src/index.js'
import assert from 'node:assert'

test('transforms:header:assert', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.remove({ column: 'x' }),
      tf.header.assert({
        headers: ['x']
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
      ['a' , 'x'  ,'c'],
      ['2' , 'foo', ''],
      ['3' , 'bar', ''],
      ['4' , 'foo', '']
    ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform(),
    tableTransformer
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('should fail')
  } catch (err) {
    assert.ok(err instanceof TransformStepColumnsNotFoundError)
    assert.equal(err.message, 'Column(s) not found: "x"')
    err.report()
  }
})
