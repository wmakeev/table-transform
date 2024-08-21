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
  const interval = setInterval(() => {}, 10000)

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

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
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

  clearInterval(interval)
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

test('transforms:splitIn #3', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.splitIn({
        splitColumns: ['key1', 'key2'],
        transformConfig: {
          outputHeader: {
            forceColumns: ['key1', 'key2', 'col1', '_row_num', 'error']
          },
          transforms: [
            tf.column.add({
              columnName: '_row_num'
            }),
            tf.column.transform({
              columnName: '_row_num',
              expression: 'row()'
            }),
            tf.column.transform({
              columnName: '_row_num',
              expression: `
                if 'key1' == "tow" then 'bar' else 'foo' & "_" & value()
              `
            })
          ],
          errorHandle: {
            errorColumn: 'error',
            transforms: [
              tf.tapRows({
                tapFunction: chunk => {
                  chunk
                }
              }),
              tf.column.transform({
                columnName: 'error',
                expression: `name of 'error'`
              }),
              tf.tapRows({
                tapFunction: chunk => {
                  chunk
                }
              })
            ]
          }
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

    ['foo' , 'tow'  , '2'   , 'r06' ],
    ['foo' , 'tow'  , '2'   , 'r07' ],
    ['foo' , 'tow'  , '2'   , 'r08' ],

    ['foo' , 'tow'  , '2'   , 'r09' ],
    ['foo' , 'tow'  , '2'   , 'r10' ],
    ['foo' , 'tow'  , '2'   , 'r11' ],

    ['foo' , 'tow'  , '2'   , 'r12' ],
    ['foo' , 'three', '3'   , 'r13' ],
    ['foo' , 'three', '3'   , 'r14' ],

    ['foo' , 'three', '3'   , 'r15' ],
    ['foo' , 'three', '3'   , 'r16' ],
    ['foo' , 'three', '3'   , 'r17' ],

    ['foo' , 'three', '3'   , 'r18' ],
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
    ['key1', 'key2', 'col1', '_row_num', 'error'],
    ['one', '1', 'r01', 'foo_1', null],
    ['one', '1', 'r02', 'foo_2', null],
    ['one', '1', 'r03', 'foo_3', null],
    ['one', '1', 'r04', 'foo_4', null],
    [null, null, null, null, 'TransformRowExpressionError'],
    ['three', '3', 'r13', 'foo_1', null],
    ['three', '3', 'r14', 'foo_2', null],
    ['three', '3', 'r15', 'foo_3', null],
    ['three', '3', 'r16', 'foo_4', null],
    ['three', '3', 'r17', 'foo_5', null],
    ['three', '3', 'r18', 'foo_6', null]
  ])
})
