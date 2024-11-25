import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  createTableTransformer,
  FlattenTransform,
  transforms as tf
} from '../../../src/index.js'

test('Column:Derive', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.derive({
        column: 'sum',
        expression: `'a' + 'b'`
      })
    ]
  })

  const table =
    /* prettier-ignore */
    [
      [
        ['a', 'b'],
        [1, 4]
      ],
      [
        [2, 5],
        [3, 6]
      ]
    ]

  const transformedRows: string[][] = await compose(
    () => tableTransformer(table),
    new FlattenTransform()
  ).toArray()

  assert.deepEqual(
    transformedRows,
    /* prettier-ignore */
    [
      ['a', 'b', 'sum'],
      [ 1 ,  4 ,    5 ],
      [ 2 ,  5 ,    7 ],
      [ 3 ,  6 ,    9 ]
    ]
  )
})
