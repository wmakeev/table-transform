import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify/sync'
import assert from 'node:assert/strict'
import { createReadStream } from 'node:fs'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
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
} from '../../../../src/index.js'

test('sheetCell transform (case1)', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.add({
        columnName: 'row'
      }),

      transforms.column.transform({
        columnName: 'row',
        expression: 'row()'
      }),

      //#region Column1 - A1
      transforms.column.add({
        columnName: 'Col1'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1',
        testValue: 'Column1',
        targetColumn: 'Col1'
      }),
      //#endregion

      //#region Column2 - B2:D11
      transforms.column.add({
        columnName: 'Col2'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'B2:D11',
        testValue: 'Column2',
        targetColumn: 'Col2'
      }),
      //#endregion

      //#region "Column3:" - B2:D11
      transforms.column.add({
        columnName: 'Col3'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'C1:E4',
        testValue: 'Column3:',
        offset: 'R[1]C[1]',
        targetColumn: 'Col3'
      }),
      //#endregion

      //#region "Const4:" - E4:G7
      transforms.column.add({
        columnName: 'Col4'
      }),

      transforms.column.sheetCell({
        type: 'CONSTANT',
        range: 'E4:G7',
        testValue: 'Const4:',
        offset: 'R[-3]C[2]',
        targetColumn: 'Col4'
      }),
      //#endregion

      transforms.column.sheetCell({
        type: 'ASSERT',
        range: 'H7:J9',
        testValue: 'Assert1'
      }),

      //#region "foo"
      transforms.column.add({
        columnName: 'Col5'
      }),

      transforms.column.sheetCell({
        type: 'CONSTANT',
        range: 'H7:J9',
        testValue: 'foo',
        targetColumn: 'Col5',
        isOptional: true
      }),
      //#endregion

      transforms.column.select({
        columns: ['row', 'Col1', 'Col2', 'Col3', 'Col4', 'Col5']
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/column/sheetCell/sheetCell1_in.csv'
      ),
      {
        highWaterMark: 16 * 1024,
        encoding: 'utf8'
      }
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.take(100).toArray()

  const actualCsv = stringify(transformedRows)

  // // DEBUG
  // await writeFile(
  //   path.join(process.cwd(), 'test/transforms/column/sheetCell/sheetCell1_out.csv'),
  //   actualCsv
  // )

  const expectedCsv = await readFile(
    path.join(
      process.cwd(),
      'test/transforms/column/sheetCell/sheetCell1_out.csv'
    ),
    'utf-8'
  )

  assert.equal(actualCsv, expectedCsv)
})

test('sheetCell transform (case1) - cell not found', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.add({
        columnName: 'Col1'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1',
        testValue: 'foo',
        targetColumn: 'Col1'
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/column/sheetCell/sheetCell1_in.csv'
      ),
      {
        highWaterMark: 16 * 1024,
        encoding: 'utf8'
      }
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  try {
    await transformedRowsStream.take(100).toArray()
    assert.fail('should throw error')
  } catch (err) {
    assert.ok(err instanceof Error)
    assert.equal(err.message, 'Cell "foo" in "A1" range not found')
  }
})

test('sheetCell transform (case1) - assert', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.add({
        columnName: 'Col1'
      }),

      transforms.column.sheetCell({
        type: 'ASSERT',
        range: 'A2:B4',
        testValue: 'foo'
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/column/sheetCell/sheetCell1_in.csv'
      ),
      {
        highWaterMark: 16 * 1024,
        encoding: 'utf8'
      }
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  try {
    await transformedRowsStream.take(100).toArray()
    assert.fail('should throw error')
  } catch (err) {
    assert.ok(err instanceof Error)
    assert.equal(err.message, 'Cell "foo" in "A2:B4" range not found')
  }
})

test('sheetCell transform (case2)', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.add({
        columnName: 'col1'
      }),

      transforms.column.add({
        columnName: 'col2'
      }),

      transforms.column.add({
        columnName: 'col3'
      }),

      transforms.column.add({
        columnName: 'col4'
      }),

      transforms.column.add({
        columnName: 'col5'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1:K1',
        testValue: 'One',
        testOperation: 'EQUAL',
        targetColumn: 'col1'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1:K1',
        testValue: 'Tow',
        testOperation: 'STARTS_WITH',
        targetColumn: 'col2'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1:K1',
        testValue: 'Три',
        targetColumn: 'col3'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1:K1',
        testValue: 'Четыре',
        testOperation: 'STARTS_WITH',
        targetColumn: 'col4'
      }),

      transforms.column.sheetCell({
        type: 'HEADER',
        range: 'A1:K1',
        testValue: ' 5',
        testOperation: 'INCLUDES',
        targetColumn: 'col5'
      }),

      transforms.column.select({
        columns: ['col1', 'col2', 'col3', 'col4', 'col5']
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/column/sheetCell/sheetCell2.csv'
      ),
      'utf8'
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['col1', 'col2', 'col3', 'col4', 'col5'],
    [null, null, null, null, null],
    ['11', '', '31', '', ''],
    ['12', '21', '', '', ''],
    ['13', '', '32', '', '55'],
    ['', '22', '33', '', ''],
    ['', '', '', '', ''],
    ['', '23', '', '', '']
  ])
})
