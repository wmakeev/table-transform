import assert from 'node:assert'
import {
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

test('transforms:channel #1', async () => {
  const CHANNEL_NAME = 'TEST_CHAN'

  const tableTransformer = createTableTransformer({
    transforms: [
      tf.mergeFromChannel({
        channelName: CHANNEL_NAME
      }),

      tf.column.transform({
        column: 'index',
        expression: `value() + 1`
      }),

      tf.takeWhile({
        column: 'index',
        expression: `value() < 5`
      }),

      tf.forkToChannel({
        channelName: CHANNEL_NAME
      })
    ]
  })

  const sourceChunks: TableRow[][] = [[['index'], [[0]]]]

  const transformedRowsStream = tableTransformer(sourceChunks)

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
  }

  assert.deepEqual(result, [[['index']], [[1]], [[2]], [[3]], [[4]]])
})

test('transforms:channel #2', async () => {
  const CHANNEL_NAME = 'TEST_CHAN'

  const tableTransformer = createTableTransformer({
    transforms: [
      tf.mergeFromChannel({
        channelName: CHANNEL_NAME
      }),

      tf.column.add({
        column: 'col1',
        defaultValue: 'col1'
      }),

      tf.column.assert({
        message: 'index column should be number',
        column: 'col1',
        expression: `value() == "col1"`
      }),

      tf.column.transform({
        column: 'index',
        expression: `value() + 1`
      }),

      tf.takeWhile({
        column: 'index',
        expression: `value() < 5`
      }),

      tf.column.select({
        columns: ['col1', 'index']
      }),

      tf.forkToChannel({
        channelName: CHANNEL_NAME
      })
    ]
  })

  const source = [['index'], [[0]]]

  // Use compose to pass channel source to trnasformer
  const transformedRowsStream = compose(
    source,
    new ChunkTransform(),
    tableTransformer,
    new FlattenTransform()
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
  }

  assert.deepEqual(result, [
    ['col1', 'index'],
    ['col1', 1],
    ['col1', 2],
    ['col1', 3],
    ['col1', 4]
  ])
})

test('transforms:channel (merge channel from fork)', async () => {
  const CHANNEL_NAME = 'TEST_CHAN'

  const tableTransformer = createTableTransformer({
    transforms: [
      tf.header.assert({
        headers: ['index']
      }),

      tf.mergeFromChannel({
        channelName: CHANNEL_NAME
      }),

      tf.column.tapValue({
        column: 'index',
        tapFunction: val => {
          assert.ok(typeof val === 'number', 'index should to be number')
        }
      }),

      tf.tapRows({
        tapFunction: chunk => {
          chunk
        }
      }),

      tf.fork({
        transformConfig: {
          transforms: [
            tf.column.transform({
              column: 'index',
              expression: `value() + 1`
            }),

            tf.forkToChannel({
              channelName: CHANNEL_NAME
            })
          ]
        }
      }),

      tf.takeWhile({
        column: 'index',
        expression: `value() < 5`
      })
    ]
  })

  const sourceChunks: TableRow[][] =
    /* prettier-ignore */
    [
      [
        ['index'],
        [0]
      ]
    ]

  const transformedRowsStream = tableTransformer(sourceChunks)

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
  }

  assert.deepEqual(
    result,
    /* prettier-ignore */
    [
      [['index']],
      [[0]],
      [[1]],
      [[2]],
      [[3]],
      [[4]]
    ]
  )
})
