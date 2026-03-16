import assert from 'node:assert/strict'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { test, suite } from 'node:test'
import {
  FlattenTransform,
  TableRow,
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { createTestContext } from '../_common/TestContext.js'

suite('transforms:pre/append', () => {
  test('simple', async () => {
    const tableTransformConfig: TableTransformConfig = {
      transforms: [
        tf.prepend({
          table: [['x'], [1]]
        }),

        tf.append({
          table: [['x'], [7]]
        })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [['x'], [2], [3]],
      [[4], [5], [6]]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['x'],
      [1],
      [2],
      [3],
      [4],
      [5],
      [6],
      [7]
    ])
  })

  test('complex', async () => {
    const tableTransformConfig: TableTransformConfig = {
      context: createTestContext(),
      transforms: [
        tf.column.derive({
          column: '№',
          expression: `row()`
        }),

        // Test correct column normalization for rows concat
        tf.column.remove({
          column: '#'
        }),

        tf.prepend({
          table: [
            ['e', 'b', 'a', 'c'],
            ['', true, '1', 'bar'],
            ['42', 'foo', '2', true],
            ['', [5, null], '3', null]
          ],

          transformConfig: {
            transforms: [
              tf.column.unroll({
                column: 'b',
                strictArrayColumn: false
              }),
              tf.column.renameMany({
                renames: {
                  a: '№',
                  b: 'Value'
                }
              })
            ]
          }
        }),

        tf.append({
          table: [
            ['b', 'e', 'a', 'c'],
            [true, '', '4', 'baz'],
            ['bar', '43', '5', false],
            [[6, 7], '', '6', null]
          ],

          transformConfig: {
            transforms: [
              tf.column.unroll({
                column: 'b',
                strictArrayColumn: false
              }),
              tf.column.renameMany({
                renames: {
                  a: '№',
                  b: 'Value',
                  e: 'Text'
                }
              })
            ]
          }
        }),

        tf.column.rename({
          column: 'c',
          newName: 'Text'
        }),

        tf.column.select({ columns: ['№', 'Value', 'Text'] })
      ]
    }

    const sourceDataChunks: TableRow[][] = [
      [
        ['#', 'Value', 'c'],
        ['-', 6, 'foo1', 'text'],
        ['-', 5, 'foo2', 'text']
      ],
      [
        ['-', 2, 'foo3', 'text'],
        ['-', 4, 'foo4', 'text'],
        ['-', 1, 'foo5', 'text']
      ]
    ]

    const transformedRowsStream: Readable = compose(
      sourceDataChunks,
      createTableTransformer(tableTransformConfig),
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['№', 'Value', 'Text'],
      ['1', true, 'bar'],
      ['2', 'foo', true],
      ['3', 5, null],
      ['3', null, null],
      [1, 6, 'foo1'],
      [2, 5, 'foo2'],
      [3, 2, 'foo3'],
      [4, 4, 'foo4'],
      [5, 1, 'foo5'],
      ['4', true, 'baz'],
      ['5', 'bar', false],
      ['6', 6, null],
      ['6', 7, null]
    ])
  })
})
