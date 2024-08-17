import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import { TransformRowExpressionError } from '../../../src/errors/index.js'
import {
  ChunkTransform,
  FlattenTransform,
  createTableTransformer,
  transforms
} from '../../../src/index.js'

test('transforms:column:filter', async t => {
  await t.test('complex filter', async () => {
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
              isEmptyArr: (arr: Array<any>) => {
                return Array.isArray(arr) && arr.length === 0
              }
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

  await t.test('filter column error', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.filter({
          columnName: 'B',
          expression: `'foo'`
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['1' , '2', '3'],
      [''  , '' , '' ],
      [''  , '' , '' ],
      [''  , '' , '' ],
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 2 }),
      tableTransformer,
      new FlattenTransform()
    )

    try {
      await transformedRowsStream.toArray()
    } catch (err) {
      assert.ok(err instanceof TransformRowExpressionError)

      assert.deepEqual(err.chunk, [
        ['1', '2', '3'],
        ['', '', '']
      ])

      assert.equal(err.column, 'B')
      assert.equal(err.columnIndex, 1)
      assert.equal(err.expression, "'foo'")
      assert.equal(err.header.length, 3)
      assert.deepEqual(err.row, ['1', '2', '3'])
      assert.equal(err.rowIndex, 0)
      assert.equal(err.rowNum, 1)
      assert.deepEqual(err.rowRecord, {
        A: '1',
        B: '2',
        C: '3'
      })
      assert.equal(err.stepName, 'Column:Filter')
      assert.equal(err.message, 'Column(s) not found: "foo"')

      err.report()
    }
  })

  await t.test('filter row error', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.filter({
          expression: `'foo'`
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['1' , '2', '3'],
      [''  , '' , '' ],
      [''  , '' , '' ],
      [''  , '' , '' ],
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform({ batchSize: 2 }),
      tableTransformer,
      new FlattenTransform()
    )

    try {
      await transformedRowsStream.toArray()
    } catch (err) {
      assert.ok(err instanceof TransformRowExpressionError)

      assert.deepEqual(err.chunk, [
        ['1', '2', '3'],
        ['', '', '']
      ])

      assert.equal(err.column, undefined)
      assert.equal(err.columnIndex, null)
      assert.equal(err.expression, "'foo'")
      assert.equal(err.header.length, 3)
      assert.deepEqual(err.row, ['1', '2', '3'])
      assert.equal(err.rowIndex, 0)
      assert.equal(err.rowNum, 1)
      assert.deepEqual(err.rowRecord, {
        A: '1',
        B: '2',
        C: '3'
      })
      assert.equal(err.stepName, 'Column:Filter')
      assert.equal(err.message, 'Column(s) not found: "foo"')

      err.report()
    }
  })
})
