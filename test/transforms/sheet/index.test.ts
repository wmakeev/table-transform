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
import { suite, test } from 'node:test'
import {
  ChunkTransform,
  FlattenTransform,
  createTableTransformer,
  transforms
} from '../../../src/index.js'
import { createTestContext } from '../../_common/TestContext.js'

suite('transforms:cell:sheetCell', () => {
  test('case1', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'row'
        }),

        transforms.column.transform({
          column: 'row',
          expression: 'row()'
        }),

        //#region Column1 - A1
        transforms.column.add({
          column: 'Col1'
        }),

        transforms.sheet.column({
          range: 'A1',
          testValue: 'Column1',
          targetColumn: 'Col1'
        }),
        //#endregion

        //#region Column2
        transforms.column.add({
          column: 'Col2'
        }),

        transforms.sheet.column({
          range: 'B2:D11',
          testValue: 'Column2',
          targetColumn: 'Col2'
        }),
        //#endregion

        //#region "Column3:" - B2:D11
        transforms.column.add({
          column: 'Col3'
        }),

        transforms.sheet.column({
          range: 'C1:E4',
          testValue: 'Column3:',
          offset: 'R[1]C[1]',
          targetColumn: 'Col3'
        }),
        //#endregion

        //#region "Const4:" - E4:G7
        transforms.column.add({
          column: 'Col4'
        }),

        transforms.sheet.constant({
          range: 'E4:G7',
          testValue: 'Const4:',
          offset: 'R[-3]C[2]',
          targetColumn: 'Col4'
        }),
        //#endregion

        //#region ANY - G1:I3
        transforms.column.addArray({
          column: 'Col5',
          length: 2
        }),

        transforms.sheet.column({
          range: 'G1:I3',
          targetColumn: 'Col5',
          targetArrColumnIndex: 1
          // Параметры будут выведены по умолчанию
          // testOperation: 'ANY',
          // testValue: undefined,
        }),
        //#endregion

        transforms.sheet.assert({
          range: 'H7:J9',
          testValue: 'Assert1'
        }),

        transforms.sheet.assert({
          range: 'B1',
          testOperation: 'EMPTY'
        }),

        //#region "foo"
        transforms.column.add({
          column: 'Col6'
        }),

        transforms.sheet.constant({
          range: 'H7:J9',
          testValue: 'foo',
          targetColumn: 'Col6',
          isOptional: true
        }),
        //#endregion

        transforms.column.select({
          columns: [
            'row',
            'Col1',
            'Col2',
            'Col3',
            'Col4',
            'Col5',
            'Col5',
            'Col6'
          ]
        })
      ]
    })

    const transformedRowsStream: Readable = compose(
      createReadStream(
        path.join(process.cwd(), 'test/transforms/sheet/sheetCell1_in.csv'),
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

    // DEBUG
    // await writeFile(
    //   path.join(
    //     process.cwd(),
    //     'test/transforms/sheet/sheetCell1_out.csv'
    //   ),
    //   actualCsv
    // )

    const expectedCsv = await readFile(
      path.join(process.cwd(), 'test/transforms/sheet/sheetCell1_out.csv'),
      'utf-8'
    )

    assert.equal(actualCsv, expectedCsv)
  })

  test('case1 - shifted', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'row'
        }),

        transforms.column.transform({
          column: 'row',
          expression: 'row()'
        }),

        //#region Column1 - A1
        transforms.column.add({
          column: 'Col1'
        }),

        transforms.sheet.column({
          range: 'A11',
          testValue: 'Column1',
          targetColumn: 'Col1'
        }),
        //#endregion

        //#region Column2
        transforms.column.add({
          column: 'Col2'
        }),

        transforms.sheet.column({
          range: 'B12:D21',
          testValue: 'Column2',
          targetColumn: 'Col2'
        }),
        //#endregion

        //#region "Column3:"
        transforms.column.add({
          column: 'Col3'
        }),

        transforms.sheet.column({
          range: 'C11:E14',
          testValue: 'Column3:',
          offset: 'R[1]C[1]',
          targetColumn: 'Col3'
        }),
        //#endregion

        //#region "Const4:" - E4:G7
        transforms.column.add({
          column: 'Col4'
        }),

        transforms.sheet.constant({
          range: 'E14:G17',
          testValue: 'Const4:',
          offset: 'R[-3]C[2]',
          targetColumn: 'Col4'
        }),
        //#endregion

        //#region ANY - G1:I3
        transforms.column.addArray({
          column: 'Col5',
          length: 2
        }),

        transforms.sheet.column({
          range: 'G11:I13',
          // Параметры будут выведены по умолчанию
          // testOperation: 'ANY',
          // testValue: undefined,
          targetColumn: 'Col5',
          targetArrColumnIndex: 1
        }),
        //#endregion

        transforms.sheet.assert({
          range: 'H17:J19',
          testValue: 'Assert1'
        }),

        transforms.sheet.assert({
          range: 'B11',
          testOperation: 'EMPTY'
        }),

        //#region "foo"
        transforms.column.add({
          column: 'Col6'
        }),

        transforms.sheet.constant({
          range: 'H17:J19',
          testValue: 'foo',
          targetColumn: 'Col6',
          isOptional: true
        }),
        //#endregion

        transforms.column.select({
          columns: [
            'row',
            'Col1',
            'Col2',
            'Col3',
            'Col4',
            'Col5',
            'Col5',
            'Col6'
          ]
        })
      ]
    })

    const transformedRowsStream: Readable = compose(
      createReadStream(
        path.join(
          process.cwd(),
          'test/transforms/sheet/sheetCell1_shifted_in.csv'
        ),
        {
          highWaterMark: 16 * 1024,
          encoding: 'utf8'
        }
      ),

      parse({ bom: true }),

      new ChunkTransform({ batchSize: 3 }),

      tableTransformer,

      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.take(100).toArray()

    const actualCsv = stringify(transformedRows)

    // DEBUG
    // await writeFile(
    //   path.join(
    //     process.cwd(),
    //     'test/transforms/sheet/sheetCell1_shifted_out.csv'
    //   ),
    //   actualCsv
    // )

    const expectedCsv = await readFile(
      path.join(
        process.cwd(),
        'test/transforms/sheet/sheetCell1_shifted_out.csv'
      ),
      'utf-8'
    )

    assert.equal(actualCsv, expectedCsv)
  })

  test('case1 - cell not found', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'Col1'
        }),

        transforms.sheet.column({
          range: 'A1',
          testValue: 'foo',
          targetColumn: 'Col1'
        })
      ]
    })

    const transformedRowsStream: Readable = compose(
      createReadStream(
        path.join(process.cwd(), 'test/transforms/sheet/sheetCell1_in.csv'),
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

  test('case1 - assert', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'Col1'
        }),

        transforms.sheet.assert({
          range: 'A2:B4',
          testValue: 'foo'
        })
      ]
    })

    const transformedRowsStream: Readable = compose(
      createReadStream(
        path.join(process.cwd(), 'test/transforms/sheet/sheetCell1_in.csv'),
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

  test('case2', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'col1'
        }),

        transforms.column.add({
          column: 'col2'
        }),

        transforms.column.add({
          column: 'col3'
        }),

        transforms.column.add({
          column: 'col4'
        }),

        transforms.column.add({
          column: 'col5'
        }),

        transforms.sheet.column({
          range: 'A1:K1',
          testValue: 'One',
          testOperation: 'EQUAL',
          targetColumn: 'col1'
        }),

        transforms.sheet.column({
          range: 'A1:K1',
          testValue: 'Tow',
          testOperation: 'STARTS_WITH',
          targetColumn: 'col2'
        }),

        transforms.sheet.column({
          range: 'A1:K1',
          testValue: 'Три',
          targetColumn: 'col3'
        }),

        transforms.sheet.column({
          range: 'A1:K1',
          testValue: 'Четыре',
          testOperation: 'STARTS_WITH',
          targetColumn: 'col4'
        }),

        transforms.sheet.column({
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
        path.join(process.cwd(), 'test/transforms/sheet/sheetCell2.csv'),
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

  test('range y overflow and EQUAL trim', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'col1'
        }),

        transforms.sheet.column({
          range: 'A1:B25',
          testValue: ' 23',
          testOperation: 'EQUAL',
          targetColumn: 'col1'
        }),

        transforms.column.select({
          columns: ['col1']
        })
      ]
    })

    const rows = [
      ['10', '21'],
      ['11', '22'],
      ['12', '23 '],
      ['13', '24'],
      ['14', '24'],
      ['15', '25'],
      ['16', '26'],
      ['17', '27'],
      ['18', '28'],
      ['19', '29']
    ]

    const transformedRowsStream: Readable = compose(
      rows.values(),
      new ChunkTransform({ batchSize: 3 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['col1'],
      [null],
      [null],
      [null],
      ['24'],
      ['24'],
      ['25'],
      ['26'],
      ['27'],
      ['28'],
      ['29']
    ])
  })

  test('optional range y overflow', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'col1'
        }),
        transforms.sheet.column({
          range: 'B3:B25',
          testValue: '25',
          testOperation: 'EQUAL',
          targetColumn: 'col1'
        }),

        transforms.column.add({
          column: 'col2'
        }),
        transforms.sheet.column({
          range: 'A2:B25',
          testValue: 'foo',
          testOperation: 'EQUAL',
          targetColumn: 'col2',
          isOptional: true
        }),

        transforms.column.select({
          columns: ['col1', 'col2']
        })
      ]
    })

    const rows = [
      ['10', '20'],
      ['11', '21'],
      ['12', '22'],
      ['13', '23'],
      ['14', '24'],
      ['15', '25'],
      ['16', '26'],
      ['17', '27'],
      ['18', '28'],
      ['19', '29']
    ]

    const transformedRowsStream: Readable = compose(
      rows.values(),
      new ChunkTransform({ batchSize: 3 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['col1', 'col2'],
      [null, null],
      [null, null],
      [null, null],
      [null, null],
      [null, null],
      [null, null],
      ['26', null],
      ['27', null],
      ['28', null],
      ['29', null]
    ])
  })

  test('variations', async () => {
    const getTransformerResult = async (
      range: string,
      testValue: string,
      offset: string | undefined,
      batchSize: number
    ) => {
      const tableTransformer = createTableTransformer({
        inputHeader: {
          mode: 'EXCEL_STYLE'
        },
        transforms: [
          transforms.column.add({
            column: 'col1'
          }),

          transforms.sheet.column({
            range,
            testValue,
            offset,
            testOperation: 'EQUAL',
            targetColumn: 'col1'
          }),

          transforms.column.select({
            columns: ['col1']
          })
        ]
      })

      const transformedRowsStream: Readable = compose(
        rows.values(),
        new ChunkTransform({ batchSize }),
        tableTransformer,
        new FlattenTransform()
      )

      const transformedRows = await transformedRowsStream.toArray()

      return transformedRows.flat().map(it => it ?? '')
    }

    const rows = [
      //A     B     C     D     E     F     G     H     I     J
      ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09'], // 1
      ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19'], // 2
      ['20', '21', '22', '23', '24', '25', '26', '27', '28', '29'], // 3
      ['30', '31', '32', '33', '34', '35', '36', '37', '38', '39'], // 4
      ['40', '41', '42', '43', '44', '45', '46', '47', '48', '49'], // 5
      ['50', '51', '52', '53', '54', '55', '56', '57', '58', '59'], // 6
      ['60', '61', '62', '63', '64', '65', '66', '67', '68', '69'], // 7
      ['70', '71', '72', '73', '74', '75', '76', '77', '78', '79'], // 8
      ['80', '81', '82', '83', '84', '85', '86', '87', '88', '89'], // 9
      ['90', '91', '92', '93', '94', '95', '96', '97', '98', '99'] // 10
    ]

    // TODO Добавить больше вариантов для теста
    const cases: {
      range: string
      testValue: string
      offset?: string | undefined
      result: any[] | null
    }[] = [
      {
        range: 'A1:C4',
        testValue: '11',
        offset: undefined,
        result: ['col1', '', '', '21', '31', '41', '51', '61', '71', '81', '91']
      },
      {
        range: 'H5:J10',
        testValue: '79',
        result: ['col1', '', '', '', '', '', '', '', '', '89', '99']
      },
      {
        range: 'B1:D3',
        testValue: '01',
        offset: 'RC[-1]',
        result: [
          'col1',
          '',
          '10',
          '20',
          '30',
          '40',
          '50',
          '60',
          '70',
          '80',
          '90'
        ]
      }
    ]

    for (const c of cases) {
      let result

      const label = `Range: ${c.range} Offset: ${c.offset}`

      try {
        result = await getTransformerResult(c.range, c.testValue, c.offset, 3)
      } catch (err) {
        console.debug(label)
        throw err
      }

      if (c.result == null) {
        console.log(label, JSON.stringify(result))
      } else {
        assert.deepEqual(result, c.result, label)
      }
    }
  })

  test('CONSTANT cell to column', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        transforms.column.add({
          column: 'Col1'
        }),

        transforms.sheet.constant({
          range: 'A1',
          targetColumn: 'Col1'
        }),

        transforms.sheet.constant({
          range: 'B2',
          targetColumn: 'B'
        })
      ]
    })

    const rows = [
      ['10', '20'],
      ['11', '21'],
      ['12', '22'],
      ['13', '23'],
      ['14', '24']
    ]

    const transformedRowsStream: Readable = compose(
      rows.values(),
      new ChunkTransform({ batchSize: 3 }),
      tableTransformer,
      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    assert.deepEqual(transformedRows, [
      ['A', 'B', 'Col1'],
      ['10', '20', null],
      ['11', '21', '10'],
      ['12', '21', '10'],
      ['13', '21', '10'],
      ['14', '21', '10']
    ])
  })

  // TODO Нужно больше тестов
  // TODO Разбить тесты на группы
})
