import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  FlattenTransform,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'

test('transforms:column:remove', async t => {
  await t.test('remove simple', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        tf.column.remove({
          columnName: 'A'
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['', '' , ''],
      ['', '1', ''],
      ['', '' , ''],
      ['', '2', '']
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),

      new ChunkTransform({ batchSize: 2 }),

      tableTransformer,

      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(
      transformedRows,
      /* prettier-ignore */
      [
        ['B', 'C'],
        ['' , '' ],
        ['1', '' ],
        ['' , '' ],
        ['2', '' ]
      ]
    )
  })

  await t.test('remove by index', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.remove({
          columnName: 'Col',
          colIndex: 1
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['Col', 'Foo', 'Col'],
      [''   , '1'  , ''   ],
      ['4'  , ''   , '3'  ],
      [''   , '2'  , ''   ]
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),

      new ChunkTransform({ batchSize: 2 }),

      tableTransformer,

      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(
      transformedRows,
      /* prettier-ignore */
      [
        ['Col', 'Foo'],
        [''   , '1'  ],
        ['4'  , ''   ],
        [''   , '2'  ]
      ]
    )
  })

  await t.test('remove by internal index', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.select({
          columns: ['X', 'Col', 'Foo', 'Col'],
          addMissingColumns: true
        }),
        tf.column.remove({
          columnName: 'Col',
          colIndex: 2,
          isInternalIndex: true
        }),
        tf.column.remove({
          columnName: 'X'
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['Col', 'Foo', 'Col'],
      [''   , '1'  , ''   ],
      ['4'  , ''   , '3'  ],
      [''   , '2'  , ''   ]
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),

      new ChunkTransform({ batchSize: 2 }),

      tableTransformer,

      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['Col', 'Foo'],
      ['', '1'],
      ['4', ''],
      ['', '2']
    ])
  })
})
