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
} from '../../src/index.js'

test('transforms:tapHeader', async () => {
  let tapedHeader: string[] | null = null

  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.tapHeader({
        tapFunction: header => {
          tapedHeader = header.map(h => h.name)
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

  assert.ok(tapedHeader)
  assert.deepEqual(tapedHeader, ['col1', 'col2'])
})
