import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  FlattenTransform,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { TransformAssertError } from '../../src/errors/index.js'

test('transforms:assertNotEmpty', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.filter({
        columnName: 'col2',
        expression: 'value() > 5'
      }),
      tf.assertNotEmpty()
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2'],
    ['one'  , 1     ],
    ['tow'  , 2     ],
    ['three', 3     ]
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('should fail')
  } catch (err) {
    assert.ok(err instanceof TransformAssertError)
    assert.equal(err.stepName, 'AssertNotEmpty')
  }
})
