import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  createTableTransformer,
  FlattenTransform,
  TransformStepRowError,
  transforms
} from '../../../src/index.js'

test('transforms:column:unroll', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.unroll({
        column: 'arr'
      })
    ]
  })

  /* prettier-ignore */
  const table = [
    [
      ['a' , 'b' , 'arr' , 'c' ],
      ['a1', 'b1', [1, 2], 'c1'],
      ['a2', 'b2', [3, 4], 'c2'],
      ['a3', 'b3', [5]   , 'c3'],
    ],
    [
      ['a4', 'b4', 6     , 'c4'],
      ['a5', 'b5', []    , 'c5'],
    ]
  ]

  const transformedRows: string[][] = await compose(
    () => tableTransformer(table),
    new FlattenTransform()
  ).toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['a' , 'b' , 'arr', 'c' ],
    ['a1', 'b1', 1    , 'c1'],
    ['a1', 'b1', 2    , 'c1'],
    ['a2', 'b2', 3    , 'c2'],
    ['a2', 'b2', 4    , 'c2'],
    ['a3', 'b3', 5    , 'c3'],
    ['a4', 'b4', 6    , 'c4']
  ])
})

test('transforms:column:unroll (strict)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.unroll({
        column: 'arr',
        strictArrayColumn: true
      })
    ]
  })

  /* prettier-ignore */
  const table = [
    [
      ['a' , 'b' , 'arr' , 'c' ],
      ['a1', 'b1', [1, 2], 'c1'],
      ['a3', 'b3', [5]   , 'c3'],
    ],
    [
      ['a4', 'b4', []    , 'c4'],
      ['a5', 'b5', 6     , 'c5'],
    ]
  ]

  try {
    await compose(() => tableTransformer(table)).toArray()
  } catch (err) {
    assert.ok(err instanceof TransformStepRowError)
    assert.equal(err.rowIndex, 1)
    assert.equal(err.columnIndex, 2)
    err.report()
  }
})

test('transforms:column:unroll (empty)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.unroll({
        column: 'a'
      })
    ]
  })

  /* prettier-ignore */
  const table = [
    [
      ['a'],
      [[]],
    ]
  ]

  const transformedRows: string[][] = await compose(() =>
    tableTransformer(table)
  ).toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
  [
    [
      'a',
    ],
  ],
])
})
