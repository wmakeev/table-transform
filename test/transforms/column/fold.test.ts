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

suite('transforms:column:fold', () => {
  test('to object', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.fold({
          columns: ['a', 'b', 'c'],
          targetColumn: 'folded'
        })
      ]
    })

    /* prettier-ignore */
    const table = [
      [
        ['a' , 'b' , 'c' ],
        ['a1', 'b1', undefined],
        ['a2', null, 'c2'],
      ],
      [
        ['a3', 'b3', 3],
        ['a4', {}, 'c4']
      ]
    ]

    const transformedRows: string[][] = await compose(
      () => tableTransformer(table),
      new FlattenTransform()
    ).toArray()

    assert.deepEqual(transformedRows, [
      ['a', 'b', 'c', 'folded'],
      [
        'a1',
        'b1',
        undefined,
        {
          a: 'a1',
          b: 'b1',
          c: undefined
        }
      ],
      [
        'a2',
        null,
        'c2',
        {
          a: 'a2',
          b: null,
          c: 'c2'
        }
      ],
      [
        'a3',
        'b3',
        3,
        {
          a: 'a3',
          b: 'b3',
          c: 3
        }
      ],
      [
        'a4',
        {},
        'c4',
        {
          a: 'a4',
          b: {},
          c: 'c4'
        }
      ]
    ])
  })

  test('to array and delete', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.fold({
          columns: ['a', 'b', 'c'],
          targetColumn: 'folded',
          foldToArray: true,
          removeColumns: true
        })
      ]
    })

    /* prettier-ignore */
    const table = [
      [
        ['a' , 'b' , 'c' ],
        ['a1', 'b1', undefined],
        ['a2', null, 'c2'],
      ],
      [
        ['a3', 'b3', 3],
        ['a4', {}, 'c4']
      ]
    ]

    const transformedRows: string[][] = await compose(
      () => tableTransformer(table),
      new FlattenTransform()
    ).toArray()

    assert.deepEqual(transformedRows, [
      ['folded'],
      [['a1', 'b1', undefined]],
      [['a2', null, 'c2']],
      [['a3', 'b3', 3]],
      [['a4', {}, 'c4']]
    ])
  })

  // TODO Добавить больше тестов для проверки крайних случаев
})
