import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  FlattenTransform,
  TableRow,
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { createTestContext } from '../_common/TestContext.js'

test('transforms:compose', async () => {
  const getComposed = (value: string) => {
    return tf.compose({
      transforms: [
        tf.forkAndMerge({
          outputColumns: ['row_num', 'value'],
          transformConfigs: [
            {},
            {
              transforms: [
                tf.column.transform({
                  column: 'value',
                  expression: `"fork1 - " & value()`
                })
              ]
            },
            {
              transforms: [
                tf.column.transform({
                  column: 'value',
                  expression: `"fork2 - " & value()`
                })
              ]
            }
          ]
        }),

        tf.column.filter({
          column: 'row_num',
          expression: `value() != 2`
        }),

        tf.column.add({
          column: 'new',
          defaultValue: value
        })
      ]
    })
  }

  const tableTransformConfig: TableTransformConfig = {
    context: createTestContext(),
    outputHeader: { skip: true },
    transforms: [
      tf.take({
        count: 5
      }),

      getComposed('foo'),

      tf.column.transform({
        column: 'value',
        expression: `if value() == 6 then "aaa" else value()`
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
      [5, 6],
      [6, 1]
    ]
  ]

  const transformedRowsStream: Readable = compose(
    sourceDataChunks,
    createTableTransformer(tableTransformConfig),
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  const sortedResult = transformedRows.sort((a, b) => {
    const _a = a.join()
    const _b = b.join()
    if (_a > _b) return 1
    if (_a < _b) return -1
    return 0
  })

  assert.deepEqual(sortedResult, [
    [1, 'aaa', 'foo'],
    [1, 'fork1 - 6', 'foo'],
    [1, 'fork2 - 6', 'foo'],
    [3, 5, 'foo'],
    [3, 'fork1 - 5', 'foo'],
    [3, 'fork2 - 5', 'foo'],
    [4, 2, 'foo'],
    [4, 'fork1 - 2', 'foo'],
    [4, 'fork2 - 2', 'foo'],
    [5, 'aaa', 'foo'],
    [5, 'fork1 - 6', 'foo'],
    [5, 'fork2 - 6', 'foo']
  ])
})
