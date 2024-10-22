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

test('transforms:column:unnest', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.select({
        columns: ['c', 'b', 'a', 'obj']
      }),
      tf.column.remove({
        column: 'b'
      }),
      tf.column.unnest({
        column: 'obj',
        fields: ['foo', 'bar', 'toString']
      }),
      tf.column.select({
        columns: ['foo', 'bar', 'toString']
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
    ]
  ]

  const transformedRows: string[][] = await compose(
    () => tableTransformer(table),
    new FlattenTransform()
  ).toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['foo', 'bar', 'toString'],
    [1    , 2    , null      ],
    [null , null , null      ],
    [null , null , null      ],
    [null , null , null      ],
    [null , null , null      ]
  ])
})

// TODO Добавить больше тестов для проверки крайних случаев
