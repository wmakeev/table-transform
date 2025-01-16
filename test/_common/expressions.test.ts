import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  Context,
  FlattenTransform,
  TransformRowExpressionError,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'

test('expressions (fields access)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.derive({
        column: 'out1',
        expression: `some of 'col1'`
      }),

      tf.column.derive({
        column: 'out2',
        expression: `exist of 'col2'`
      }),

      tf.column.derive({
        column: 'out3',
        expression: `'one tow' of 'col2'`
      }),

      tf.column.derive({
        column: 'out4',
        expression: `b of a of 'col3'`
      }),

      tf.column.select({
        columns: ['out1', 'out2', 'out3', 'out4']
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2'            , 'col3'            ],
    ['one'  , { exist: true }   , { a: { b: true } }],
    ['tow'  , { 'one tow': 12 } , {}                ],
    ['tow'  , 2                 , null              ],
    ['three', true              , undefined         ]
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  const result = await transformedRowsStream.toArray()

  assert.deepEqual(
    result,
    /* prettier-ignore */
    [
      ['out1'   , 'out2'   , 'out3'   , 'out4'   ],
      [undefined, true     , undefined, true     ],
      [undefined, undefined, 12       , undefined],
      [undefined, undefined, undefined, null     ],
      [undefined, undefined, undefined, undefined]
    ]
  )
})

test('expressions (unquoted data field access error)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.derive({
        column: 'out1',
        expression: `some of col1`
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2', 'col3'],
    ['one'  , {}    , {}    ],
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('error expected')
  } catch (err) {
    assert.ok(err instanceof TransformRowExpressionError)
    assert.equal(
      err.message,
      'Field "col1" access with unquoted name - try to use quoted notation "\'col1\'"'
    )
  }
})

test('expressions (unknown symbol access error)', async () => {
  const tableTransformer = createTableTransformer({
    context: new Context().setTransformExpressionContext({
      symbols: {
        bar: 42
      }
    }),
    transforms: [
      tf.column.transform({
        column: 'col2',
        expression: `bar`
      }),
      tf.column.assert({
        message: 'test assert',
        expression: `'col2' == 42`
      }),
      tf.column.transform({
        column: 'col3',
        expression: `foo`
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2', 'col3'],
    ['one'  , {}    , {}    ],
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('error expected')
  } catch (err) {
    assert.ok(err instanceof TransformRowExpressionError)
    assert.equal(err.message, 'Symbol not found: "foo"')
    err.report()
  }
})
