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
  transforms
} from '../../../src/index.js'

test('remove header transform #1', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.remove({
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

test('remove header transform #2', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.remove({
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
