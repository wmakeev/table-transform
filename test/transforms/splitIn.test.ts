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
} from '../../src/index.js'

test('transforms:splitIn #1', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.splitIn({
        splitColumns: ['key1', 'key2'],
        transformConfig: {
          transforms: [
            tf.column.add({
              columnName: 'new'
            }),
            tf.column.transform({
              columnName: 'new',
              expression: 'row()'
            })
          ]
        }
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['key1' , 'key2', 'col1'],
    ['one'  , '1'   , 'r01' ],
    ['one'  , '1'   , 'r02' ],
    ['one'  , '1'   , 'r03' ],
    ['one'  , '1'   , 'r04' ],
    ['tow'  , '2'   , 'r05' ],
    ['three', '3'   , 'r06' ],
    ['three', '3'   , 'r07' ],
    ['three', '3'   , 'r08' ],
    ['three', '3'   , 'r09' ],
    ['three', '3'   , 'r10' ],
    ['three', '3'   , 'r11' ],
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 3 }),

    tableTransformer,

    new FlattenTransform()
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
  }

  assert.deepEqual(result, [
    ['key1', 'key2', 'col1', 'new'],
    ['one', '1', 'r01', 1],
    ['one', '1', 'r02', 2],
    ['one', '1', 'r03', 3],
    ['one', '1', 'r04', 4],
    ['tow', '2', 'r05', 1],
    ['three', '3', 'r06', 1],
    ['three', '3', 'r07', 2],
    ['three', '3', 'r08', 3],
    ['three', '3', 'r09', 4],
    ['three', '3', 'r10', 5],
    ['three', '3', 'r11', 6]
  ])
})

test('transforms:splitIn #2', async () => {
  const tableTransformer = createTableTransformer({
    outputHeader: {
      forceColumns: ['key1', 'key2', 'col1', 'new']
    },
    transforms: [
      tf.splitIn({
        splitColumns: ['key1', 'key2'],
        transformConfig: {
          transforms: [
            tf.column.add({
              columnName: 'new'
            }),
            tf.column.transform({
              columnName: 'new',
              expression: 'row()'
            })
          ]
        }
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['foo' , 'key1' , 'key2', 'col1'],
    ['foo' , 'one'  , '1'   , 'r01' ],
    ['foo' , 'one'  , '1'   , 'r02' ],
    ['foo' , 'one'  , '1'   , 'r03' ],
    ['foo' , 'one'  , '1'   , 'r04' ],
    ['foo' , 'tow'  , '2'   , 'r05' ],
    ['foo' , 'three', '3'   , 'r06' ],
    ['foo' , 'three', '3'   , 'r07' ],
    ['foo' , 'three', '3'   , 'r08' ],
    ['foo' , 'three', '3'   , 'r09' ],
    ['foo' , 'three', '3'   , 'r10' ],
    ['foo' , 'three', '3'   , 'r11' ],
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 3 }),

    tableTransformer,

    new FlattenTransform()
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
  }

  assert.deepEqual(result, [
    ['key1', 'key2', 'col1', 'new'],
    ['one', '1', 'r01', 1],
    ['one', '1', 'r02', 2],
    ['one', '1', 'r03', 3],
    ['one', '1', 'r04', 4],
    ['tow', '2', 'r05', 1],
    ['three', '3', 'r06', 1],
    ['three', '3', 'r07', 2],
    ['three', '3', 'r08', 3],
    ['three', '3', 'r09', 4],
    ['three', '3', 'r10', 5],
    ['three', '3', 'r11', 6]
  ])
})
