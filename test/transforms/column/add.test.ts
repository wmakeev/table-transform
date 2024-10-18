import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  createTableTransformer,
  FlattenTransform,
  transforms as tf
} from '../../../src/index.js'

test('add header transform #1', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      // ensure add handle reordered src columns
      tf.column.select({
        columns: ['D', 'C', 'A', 'B'],
        addMissingColumns: true
      }),
      tf.column.remove({
        columnName: 'A'
      }),
      tf.column.add({
        columnName: 'C'
      }),
      tf.column.add({
        columnName: 'F',
        defaultValue: 'F'
      }),
      tf.column.add({
        columnName: 'C',
        defaultValue: 'C'
      }),
      tf.column.add({
        columnName: 'C',
        defaultValue: 'C2',
        force: true
      })
    ]
  })

  const table =
    /* prettier-ignore */
    [
      //  A    B    C
      [
        ['a', 'b', 'c'],
        ['a', '1', '' ]
      ],
      [
        ['a', 'b', 'c'],
        ['a', '2', '' ]
      ]
    ]

  const transformedRows: string[][] = await compose(
    () => tableTransformer(table),
    new FlattenTransform()
  ).toArray()

  assert.deepEqual(
    transformedRows,
    /* prettier-ignore */
    [
      ['D' , 'C', 'F', 'B', 'C'],
      [null, 'c', 'F', 'b', 'C2'],
      [null, '' , 'F', '1', 'C2'],
      [null, 'c', 'F', 'b', 'C2'],
      [null, '' , 'F', '2', 'C2']
    ]
  )
})
