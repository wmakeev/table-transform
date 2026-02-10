import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { suite, test } from 'node:test'
import {
  ChunkTransform,
  Context,
  FlattenTransform,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { SimplexExpressionCompileProvider } from '../../_common/SimplexExpressionCompileProvider.js'

suite('transforms:column:scan', () => {
  test('scan without seed #1', async () => {
    const tableTransformer = createTableTransformer({
      context: new Context()
        .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
        .setExpressionContext({
          empty: (val: unknown) => val == null || val === ''
        }),

      transforms: [
        tf.column.derive({
          column: 'group',
          expression: 'value("items")'
        }),
        tf.column.scan({
          column: 'group',
          expression: `if empty(value("val")) then value() else prev()`
        }),
        tf.column.filter({
          column: 'val',
          expression: `not empty(value("val"))`
        }),
        tf.column.rename({
          column: 'items',
          newName: 'name'
        }),
        tf.column.select({
          columns: ['group', 'name', 'val']
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['items' , 'val'],
      ['One'   , ''   ],
      ['item 1', 1    ],
      ['Tow'   , ''   ],
      ['item 2', 2    ],
      ['item 3', 3    ],
      ['Three' , ''   ],
      ['item 4', 4    ],
      ['item 5', 5    ],
      ['item 6', 6    ],
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 2 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    /* prettier-ignore */
    assert.deepEqual(transformedRows, [
      ['group', 'name'  , 'val'],
      ['One'  , 'item 1', 1    ],
      ['Tow'  , 'item 2', 2    ],
      ['Tow'  , 'item 3', 3    ],
      ['Three', 'item 4', 4    ],
      ['Three', 'item 5', 5    ],
      ['Three', 'item 6', 6    ]
    ])
  })

  test('scan without seed #2', async () => {
    const tableTransformer = createTableTransformer({
      context: new Context()
        .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
        .setExpressionContext({
          empty: (val: unknown) => val == null || val === ''
        }),

      transforms: [
        tf.splitIn({
          keyColumns: ['id'],
          transformConfig: {
            transforms: [
              tf.column.sort({
                column: 'val'
              }),
              tf.column.add({
                column: 'scan'
              }),
              tf.column.scan({
                column: 'scan',
                expression: `if empty(prev()) then value("val") else prev()`
              })
            ]
          }
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['id', 'val'],
      ['a' , 3    ],
      ['a' , 2    ],
      ['b' , 5    ],
      ['c' , 2    ],
      ['c' , 3    ],
      ['c' , 1    ],
      ['c' , 4    ],
      ['e' , 4    ],
      ['e' , 2    ],
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 2 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    /* _prettier-ignore */
    assert.deepEqual(transformedRows, [
      ['id', 'val', 'scan'],
      ['a', 2, null],
      ['a', 3, 3],
      ['b', 5, null],
      ['c', 1, null],
      ['c', 2, 2],
      ['c', 3, 2],
      ['c', 4, 2],
      ['e', 2, null],
      ['e', 4, 4]
    ])
  })

  test('scan with seed', async () => {
    const tableTransformer = createTableTransformer({
      context: new Context()
        .setExpressionCompileProvider(new SimplexExpressionCompileProvider())
        .setExpressionContext({
          empty: (val: unknown) => val == null || val === ''
        }),

      transforms: [
        tf.column.add({
          column: 'sum'
        }),
        tf.column.scan({
          column: 'sum',
          expression: `prev() + value("val")`,
          seed: 10
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['items' , 'val'],
      ['item 1', 1    ],
      ['item 2', 2    ],
      ['item 3', 3    ],
      ['item 4', 4    ],
      ['item 5', 5    ],
      ['item 6', 6    ],
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 2 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    /* prettier-ignore */
    assert.deepEqual(transformedRows, [
      ['items', 'val', 'sum'],
      ['item 1', 1   , 11   ],
      ['item 2', 2   , 13   ],
      ['item 3', 3   , 16   ],
      ['item 4', 4   , 20   ],
      ['item 5', 5   , 25   ],
      ['item 6', 6   , 31   ]
    ])
  })
})
