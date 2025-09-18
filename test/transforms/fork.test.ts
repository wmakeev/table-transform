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
  transforms as tf
} from '../../src/index.js'
import { createTestContext } from '../_common/TestContext.js'

test('transforms:fork #1', async () => {
  const forkedChunks: TableRow[][] = []

  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      tf.column.derive({
        column: 'col3',
        expression: `'col2' * 2`
      }),

      tf.fork({
        transformConfig: {
          transforms: [
            tf.column.transform({
              column: 'col3',
              expression: `value() + 1`
            }),

            tf.wait({
              timeoutColumn: 'col3'
            }),

            tf.tapRows({
              tapFunction(chunk) {
                forkedChunks.push(chunk)
              }
            })
          ]
        }
      }),

      tf.wait({
        timeoutColumn: 'col2'
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['col1' , 'col2'],
    ['one'  ,  10   ],
    ['tow'  ,  20   ],
    ['three',  30   ]
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['col1', 'col2', 'col3'],
    ['one', 10, 20],
    ['tow', 20, 40],
    ['three', 30, 60]
  ])

  assert.deepEqual(forkedChunks, [
    [['one', 10, 21]],
    [['tow', 20, 41]],
    [['three', 30, 61]]
  ])
})

test.todo('transforms:fork (error in fork)', () => {})
