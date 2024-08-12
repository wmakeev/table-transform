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
  TableRow,
  createTableTransformer,
  transforms
} from '../../src/index.js'

test('transforms:tapRows', async () => {
  const rows: TableRow[] = []

  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.tapRows({
        tapFunction: (chunk, header) => {
          assert.deepEqual(
            ['col1', 'col2'],
            header.map(h => h.name)
          )

          rows.push(...chunk)
        }
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2'],
    ['one'  , '1'   ],
    ['tow'  , '2'   ],
    ['three', '3'   ]
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  await transformedRowsStream.toArray()

  assert.deepEqual(
    rows,
    /* prettier-ignore */
    [
      ['one', '1'],
      ['tow', '2'],
      ['three', '3']
    ]
  )
})
