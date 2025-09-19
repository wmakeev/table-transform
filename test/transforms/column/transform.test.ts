import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { suite, test } from 'node:test'
import { TransformStepRowExpressionError } from '../../../src/errors/index.js'
import {
  ChunkTransform,
  Context,
  FlattenTransform,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { createTestContext } from '../../_common/TestContext.js'
import { SimplexExpressionCompileProvider } from '../../_common/SimplexExpressionCompileProvider.js'

suite('transforms:column:transform', () => {
  test('column default value', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'col2',
          expression: '(value() ?? "") & "+"'
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

  test('column default values', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'col_',
          expression: 'values()',
          columnIndex: 1
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['col_' , 'col2',  'col_'],
      ['' , '1', ''],
      ['' , ''],
      ['3', '2', '4']
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
      ['col_', 'col2', 'col_'    ],
      [''    , '1'   , ['', '']  ],
      [''    , ''    , ['', null]],
      ['3'   , '2'   , ['3', '4']]
    ])
  })

  test('array column', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'col',
          expression: 'arrColIndex()'
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

  test('array column with specified index', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'A',
          expression: 'arrColIndex()',
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

  test('error', async () => {
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
      assert.ok(err instanceof TransformStepRowExpressionError)
    }
  })

  test('custom context', async () => {
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

  test('all transform context methods', async () => {
    const expression = `
      if      value('method') == "value()"                  then value()
      else if value('method') == "values()"                 then values()
      else if value('method') == "row()"                    then row()
      else if value('method') == "column()"                 then column()
      else if value('method') == "columns()"                then columns()
      else if value('method') == "columnIndex()"            then columnIndex()
      else if value('method') == "columnIndex('foo', 1)"    then columnIndex('foo', 1)
      else if value('method') == "columnIndex(column())"    then columnIndex(column())
      else if value('method') == "arrColIndex()"            then arrColIndex()
    `

    const tableTransformer = createTableTransformer({
      context: new Context().setExpressionCompileProvider(
        new SimplexExpressionCompileProvider()
      ),

      transforms: [
        tf.column.transform({
          column: 'foo',
          expression
        }),

        tf.column.transform({
          column: 'bar',
          expression
        }),

        tf.column.transform({
          column: 'baz',
          expression
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['method'                 , 'foo', 'bar', 'foo', 'baz'],
      ['value()'                , '1'  , '2'  , '3'  , '4'  ],
      ['values()'               , '5'  , '6'  , '7'  , '8'  ],
      ['row()'                  , ''   , ''   , ''   , ''   ],
      ['column()'               , ''   , ''   , ''   , ''   ],
      ['columns()'              , ''   , ''   , ''   , ''   ],
      ['columnIndex()'          , ''   , ''   , ''   , ''   ],
      [`columnIndex('foo', 1)`  , ''   , ''   , ''   , ''   ],
      [`columnIndex(column())`  , ''   , ''   , ''   , ''   ],
      ['arrColIndex()'          , ''   , ''   , ''   , ''   ]
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 10 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(
      transformedRows,
      /* prettier-ignore */
      [
        ['method'               , 'foo', 'bar', 'foo', 'baz'],
        ['value()'              , '1'  , '2'  , '3'  , '4'  ],
        ['values()'             ,
                                  ['5', '7'],
                                  ['6'],
                                  [['5', '7'], '7'],
                                  ['8']                     ],
        ['row()'                , 3    , 3    , 3    , 3    ],
        ['column()'             , 'foo', 'bar', 'foo', 'baz'],
        ['columns()'            ,
                                  ['method', 'foo', 'bar', 'foo', 'baz'],
                                  ['method', 'foo', 'bar', 'foo', 'baz'],
                                  ['method', 'foo', 'bar', 'foo', 'baz'],
                                  ['method', 'foo', 'bar', 'foo', 'baz']
                                                            ],

        // FIXME Должно возвращать 1, 2, 3, 4
        ['columnIndex()'        , 1    , 2    , 1    , 4    ],

        ["columnIndex('foo', 1)", 1    , 1    , 1    , 1    ],

        ['columnIndex(column())', 1    , 2    , 1    , 4    ],

        ['arrColIndex()'        , 0    , 0    , 1    , 0    ]
      ]
    )
  })
})
