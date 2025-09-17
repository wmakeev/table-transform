import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import { TransformRowExpressionError } from '../../../src/errors/index.js'
import {
  ChunkTransform,
  FlattenTransform,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { createTestContext } from '../../_common/TestContext.js'

test('transforms:column:transform (simple)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      tf.column.transform({
        column: 'col2',
        expression: 'value() & "+"'
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2',  'col3'],
    ['' , '1', ''],
    ['' , ''],
    ['3', '2', ''],
    [],
    [],
    ['' , '2', ''],
    ['3', '2', ''],
    ['' , '2', '']
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
    ['col1', 'col2', 'col3'],
    [''    , '1+'  , ''    ],
    [''    , '+'   , null  ],
    ['3'   , '2+'  , ''    ],
    [null  , '+'   , null  ],
    [null  , '+'   , null  ],
    [''    , '2+'  , ''    ],
    ['3'   , '2+'  , ''    ],
    [''    , '2+'  , ''    ]
  ])
})

test('transforms:column:transform (array)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      tf.column.transform({
        column: 'col',
        expression: 'arrayIndex()'
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col' , 'foo',  'col'],
    ['' , '1', ''],
    ['' , ''],
    ['3', '2', ''],
    [],
    [],
    ['' , '2', ''],
    ['3', '2', ''],
    ['' , '2', '']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 10 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['col', 'foo', 'col'],
    [0, '1', 1],
    [0, '', 1],
    [0, '2', 1],
    [0, null, 1],
    [0, null, 1],
    [0, '2', 1],
    [0, '2', 1],
    [0, '2', 1]
  ])
})

test('transforms:column:transform (array with selected index)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      tf.column.transform({
        column: 'A',
        expression: 'arrayIndex()',
        columnIndex: 1
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['A', 'B', 'A'],
    ['1', '2', '' ],
    ['' , '2', '3'],
    ['1', '2', '' ],
    ['' , '2', '3']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 10 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['A', 'B', 'A'],
    ['1', '2', 1],
    ['' , '2', 1],
    ['1', '2', 1],
    ['' , '2', 1]
  ])
})

test('transforms:column:transform (error)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      tf.column.transform({
        column: 'A',
        expression: 'foo',
        columnIndex: 1
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['A', 'B', 'A'],
    ['1', '2', '' ],
    ['' , '2', '3'],
    ['1', '2', '' ],
    ['' , '2', '3']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 10 }),
    tableTransformer,
    new FlattenTransform()
  )

  try {
    await transformedRowsStream.toArray()
  } catch (err) {
    assert.ok(err instanceof TransformRowExpressionError)
  }
})

test('transforms:column:transform (custom context)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext({
      foo: (val: any) => val
    }),

    transforms: [
      tf.column.transform({
        column: 'C',
        expression: 'foo(1)'
      }),

      tf.forkAndMerge({
        outputColumns: ['A', 'B', 'C', 'D'],
        transformConfigs: [
          {
            transforms: [
              tf.column.add({ column: 'D' }),
              tf.column.transform({
                column: 'D',
                expression: 'foo(11)'
              })
            ]
          },
          {
            transforms: [
              tf.column.derive({
                column: 'D',
                expression: 'foo(12)'
              })
            ]
          }
        ]
      }),

      tf.splitIn({
        keyColumns: ['D'],
        transformConfig: {
          transforms: [
            tf.column.filter({
              expression: `foo('D') > 0`
            }),

            tf.column.add({
              column: 'E',
              defaultValue: '13'
            })
          ]
        }
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['A', 'B', 'C'],
    ['1', '2', '' ],
    ['' , '2', '3']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 10 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['A', 'B', 'C', 'D', 'E'],
    ['1', '2', 1, 11, '13'],
    ['', '2', 1, 11, '13'],
    ['1', '2', 1, 12, '13'],
    ['', '2', 1, 12, '13']
  ])
})
