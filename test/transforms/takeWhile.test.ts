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

test('transforms:takeWhile', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.takeWhile({
        columnName: 'col2',
        expression: `value() < 3`
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2'],
    ['one'  , 1    ],
    ['tow'  , 2    ],
    ['three', 3    ],
    ['four' , 4    ],
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const rows = await transformedRowsStream.toArray()

  assert.deepEqual(
    rows,
    /* prettier-ignore */
    [
      ['col1' , 'col2'],
      ['one'  , 1     ],
      ['tow'  , 2     ]
    ]
  )
})
