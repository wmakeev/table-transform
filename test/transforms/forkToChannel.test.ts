import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  AsyncChannel,
  FlattenTransform,
  HeaderChunkTuple,
  chunkSourceFromChannel,
  createTableHeader,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'

test('transforms:channel #1', async () => {
  const channel = new AsyncChannel<HeaderChunkTuple>({ name: 'requests' })

  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'index',
        expression: `value() + 1`
      }),

      tf.takeWhile({
        columnName: 'index',
        expression: `value() < 5`
      }),

      tf.forkToChannel({
        channel
      })
    ]
  })

  channel.put([createTableHeader(['index']), [[0]]])

  // Direct pass channel source to trnasformer
  const transformedRowsStream = tableTransformer(
    chunkSourceFromChannel({ channel })
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
  }

  assert.deepEqual(result, [[['index']], [[1]], [[2]], [[3]], [[4]]])
})

test('transforms:channel #2', async () => {
  const channel = new AsyncChannel<HeaderChunkTuple>({ name: 'requests' })

  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'index',
        expression: `value() + 1`
      }),

      tf.takeWhile({
        columnName: 'index',
        expression: `value() < 5`
      }),

      tf.forkToChannel({
        channel
      })
    ]
  })

  channel.put([createTableHeader(['index']), [[0]]])

  // Use compose to pass channel source to trnasformer
  const transformedRowsStream = compose(
    chunkSourceFromChannel({ channel }),
    tableTransformer,
    new FlattenTransform()
  )

  const result = []

  for await (const row of transformedRowsStream) {
    result.push(row)
    // console.debug(JSON.stringify(row))
  }

  assert.deepEqual(result, [['index'], [1], [2], [3], [4]])
})