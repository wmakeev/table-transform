import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test, { suite } from 'node:test'
import {
  createTableTransformer,
  FlattenTransform,
  transforms as tf
} from '../../../src/index.js'

suite('transforms:column:unfold', () => {
  test('common case', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.select({
          columns: ['c', 'b', 'a', 'obj']
        }),
        tf.column.remove({
          column: 'b'
        }),
        tf.column.unfold({
          column: 'obj',
          fields: ['foo', 'baz', 'toString']
        }),
        tf.header.assert({
          headers: ['obj']
        }),
        tf.column.select({
          columns: ['foo', 'baz', 'toString']
        })
      ]
    })

    /* prettier-ignore */
    const table = [
    [
      ['a' , 'b' , 'obj'                   , 'c' ],
      ['a1', 'b1', {foo: 1, bar: 2, baz: 3}, 'c1'],
      ['a2', 'b2', [3, 4]                  , 'c2'],
      ['a3', 'b3', null                    , 'c3'],
    ],
    [
      ['a4', 'b4', {}                      , 'c4'],
      ['a5', 'b5', 42                      , 'c5'],
      ['a6', 'b6', undefined               , 'c6'],
    ]
  ]

    const transformedRows: string[][] = await compose(
      () => tableTransformer(table),
      new FlattenTransform()
    ).toArray()

    assert.deepEqual(transformedRows, [
      ['foo', 'baz', 'toString'],
      [1, 3, undefined],
      [3, 4, undefined],
      [undefined, undefined, undefined],
      [undefined, undefined, undefined],
      [undefined, undefined, undefined],
      [undefined, undefined, undefined]
    ])
  })

  test('overlap and remove', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.unfold({
          column: 'a',
          fields: ['c', 'd'],
          removeColumn: true
        })
      ]
    })

    const table = [
      [
        ['a', 'b', 'c'],
        [[1, 2], 'b1', 'c1'],
        ['a2', 'b2', 'c2'],
        [{ c: '', d: 4 }, 'b3', 'c3'],
        [{ c: null }, 'b4', 'c4']
      ]
    ]

    const transformedRows: string[][] = await compose(
      () => tableTransformer(table),
      new FlattenTransform()
    ).toArray()

    assert.deepEqual(transformedRows, [
      ['b', 'c', 'd'],
      ['b1', 1, 2],
      ['b2', undefined, undefined],
      ['b3', '', 4],
      ['b4', null, undefined]
    ])
  })

  // TODO Добавить больше тестов для проверки крайних случаев
})
