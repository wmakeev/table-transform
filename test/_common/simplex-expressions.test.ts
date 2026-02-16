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
  TransformStepColumnNotFoundError,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { SimplexExpressionCompileProvider } from './SimplexExpressionCompileProvider.js'
import { ExpressionError } from 'simplex-lang'

test('Simplex expressions (fields access)', async () => {
  const tableTransformer = createTableTransformer({
    context: new Context()
      .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
      .setExpressionContext({
        get: (val: any, field: any) => val[field],
        isObj: (val: any) => typeof val === 'object',
        safeObj: (val: any) => (typeof val === 'object' ? val : {})
      }),
    transforms: [
      tf.column.derive({
        column: 'out1',
        expression: `value('col1') | get(%, 'some')`
      }),

      tf.column.derive({
        column: 'out2',
        expression: `value('col2') | get(%, 'exist')`
      }),

      tf.column.derive({
        column: 'out3',
        expression: `value('col2') | if isObj(%) then %['one tow'] else undefined`
      }),

      tf.column.derive({
        column: 'out4',
        expression: `value('col3') | safeObj(%).a.b`
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
      [undefined, undefined, undefined, undefined],
      [undefined, undefined, undefined, undefined]
    ]
  )
})

test('Simplex expressions (unknown symbol access error)', async () => {
  const tableTransformer = createTableTransformer({
    context: new Context()
      .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
      .setExpressionContext({
        bar: 42
      }),
    transforms: [
      tf.column.transform({
        column: 'col2',
        expression: `value('bar')`
      }),
      tf.column.assert({
        message: 'test assert',
        expression: `value('col2') == 42`
      }),
      tf.column.transform({
        column: 'col3',
        expression: `value('foo')`
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
    assert.ok(err instanceof ExpressionError)
    assert.ok(err.cause instanceof TransformStepColumnNotFoundError)
    assert.equal(err.message, 'Column not found: "bar"')
    err.cause.report()
  }
})

test('Simplex expressions (unknown symbol access error)', async () => {
  const context = new Context()
    .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
    .setExpressionContext({
      bar: 42,
      try:
        (fn: any) =>
        (...args: any[]) => {
          try {
            return fn(...args)
          } catch (err: any) {
            return err.message
          }
        }
    })

  const tableTransformer = createTableTransformer({
    context,
    transforms: [
      tf.column.transform({
        column: 'col2',
        expression: `try(value)('bar')`
      }),
      tf.column.assert({
        message: 'test assert',
        expression: `value('col2') != 42`
      }),
      tf.column.transform({
        column: 'col3',
        expression: `value('foo')`
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
    const result = await transformedRowsStream.toArray()
    result
    assert.fail('error expected')
  } catch (err) {
    assert.ok(err instanceof ExpressionError)
    assert.ok(err.cause instanceof TransformStepColumnNotFoundError)
    assert.equal(err.message, 'Column not found: "foo"')
    err.cause.report()
  }
})
