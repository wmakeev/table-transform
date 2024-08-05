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
} from '../../../src/index.js'

test('filter transform', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.filter({
        columnName: 'B',
        expression: 'value() != "1"'
      }),

      transforms.column.filter(
        {
          expression: `
            value() == NULL and
            isEmptyArr(values()) and
            empty(value("C")) and
            A != "3" and
            not empty('B')
          `
        },
        {
          symbols: {
            NULL: null,
            isEmptyArr: (arr: Array<any>) =>
              Array.isArray(arr) && arr.length === 0
          }
        }
      )
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['' , '' , ''],
    ['' , '1', ''],
    ['' , '' , ''],
    ['3', '2', ''],
    ['' , '2', ''],
    ['3', '2', ''],
    ['' , '2', '']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['A', 'B', 'C'],
    ['', '2', ''],
    ['', '2', '']
  ])
})
