import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { suite, test } from 'node:test'
import {
  TableRow,
  TableTransformConfig,
  TransformStepParameterError,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { FlattenTransform } from '../../src/index.js'

suite('transforms:skip', () => {
  test('skip parameter error #1', async () => {
    try {
      tf.skip({ count: -1 })
      assert.fail('Error expected')
    } catch (err) {
      assert.ok(err instanceof TransformStepParameterError)
    }
  })

  test('skip parameter error #1', async () => {
    try {
      tf.skip({
        // @ts-expect-error test case
        count: 'foo'
      })
      assert.fail('Error expected')
    } catch (err) {
      assert.ok(err instanceof TransformStepParameterError)
    }
  })

  test('skip #1', async () => {
    const tableTransformConfig: TableTransformConfig = {
      transforms: [
        tf.skip({
          count: 2
        })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [
        ['row_num', 'value'],
        [1, 6],
        [2, 3],
        [3, 5]
      ],
      [
        [4, 2],
        [5, 4],
        [6, 1]
      ]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['row_num', 'value'],
      [3, 5],
      [4, 2],
      [5, 4],
      [6, 1]
    ])
  })

  test('skip #2', async () => {
    const tableTransformConfig: TableTransformConfig = {
      transforms: [
        tf.skip({
          count: 3
        })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [
        ['row_num', 'value'],
        [1, 6],
        [2, 3],
        [3, 5]
      ],
      [
        [4, 2],
        [5, 4],
        [6, 1]
      ]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['row_num', 'value'],
      [4, 2],
      [5, 4],
      [6, 1]
    ])
  })

  test('skip #3', async () => {
    const tableTransformConfig: TableTransformConfig = {
      transforms: [
        tf.skip({
          count: 4
        })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [
        ['row_num', 'value'],
        [1, 6],
        [2, 3],
        [3, 5]
      ],
      [
        [4, 2],
        [5, 4],
        [6, 1]
      ]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['row_num', 'value'],
      [5, 4],
      [6, 1]
    ])
  })

  test('skip 0', async () => {
    const tableTransformConfig: TableTransformConfig = {
      transforms: [
        tf.skip({
          count: 0
        })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [
        ['row_num', 'value'],
        [1, 6],
        [2, 3],
        [3, 5]
      ],
      [
        [4, 2],
        [5, 4],
        [6, 1]
      ]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['row_num', 'value'],
      [1, 6],
      [2, 3],
      [3, 5],
      [4, 2],
      [5, 4],
      [6, 1]
    ])
  })
})
