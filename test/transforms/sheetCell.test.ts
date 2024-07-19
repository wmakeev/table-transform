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
} from '../../src/index.js'

test('sheetCell transform #1', async () => {
  const csvTransformer = createTableTransformer({
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
    ],
    prependHeaders: 'EXCEL_STYLE'
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/sheetCell1_in.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    csvTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.take(100).toArray()

  const actualCsv = stringify(transformedRows)

  // // DEBUG
  // await writeFile(
  //   path.join(process.cwd(), 'test/cases/sheetCell1_out.csv'),
  //   actualCsv
  // )

  const expectedCsv = await readFile(
    path.join(process.cwd(), 'test/cases/sheetCell1_out.csv'),
    'utf-8'
  )

  assert.equal(actualCsv, expectedCsv)
})

test('sheetCell transform #1 (cell not found)', async () => {
  const csvTransformer = createTableTransformer({
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
    ],
    prependHeaders: 'EXCEL_STYLE'
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/sheetCell1_in.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    csvTransformer,

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

test('sheetCell transform #1 (assert)', async () => {
  const csvTransformer = createTableTransformer({
    transforms: [
      transforms.column.add({
        columnName: 'Col1'
      }),

      transforms.column.sheetCell({
        type: 'ASSERT',
        range: 'A2:B4',
        testValue: 'foo'
      })
    ],
    prependHeaders: 'EXCEL_STYLE'
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/sheetCell1_in.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    csvTransformer,

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
