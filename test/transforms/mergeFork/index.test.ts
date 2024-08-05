import { parse } from 'csv-parse'
import assert from 'node:assert/strict'
import { createReadStream } from 'node:fs'
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
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { csvSourceProvider } from '../../helpers/index.js'

test('transforms:mergeFork #1', async () => {
  const transformConfig: TableTransfromConfig = {
    transforms: [
      tf.mergeFork({
        outputColumns: ['code', 'name'],
        transformConfigs: [
          {
            transforms: [
              tf.tapHeader({
                tapFunction: _header => {
                  _header
                }
              }),
              tf.tapRows({
                tapFunction: _chunk => {
                  _chunk
                }
              }),

              tf.column.add({ columnName: 'code', defaultValue: 'one' }),
              tf.column.transform({
                columnName: 'name',
                expression: 'value() & " 1"'
              })
            ]
          },
          {
            transforms: [
              tf.column.add({ columnName: 'code', defaultValue: 'tow' }),
              tf.column.transform({
                columnName: 'name',
                expression: 'value() & " 2"'
              })
            ]
          },
          {
            transforms: [
              tf.column.add({ columnName: 'code', defaultValue: 'three' }),
              tf.column.transform({
                columnName: 'name',
                expression: 'value() & " 3"'
              })
            ]
          }
        ]
      })
    ]
  }

  const transformer = createTableTransformer(transformConfig)

  const resultGen = transformer(
    /* prettier-ignore */
    [
      [
        ['name'],
        ['foo'],
      ],
      [
        ['bar'],
        ['baz'],
      ]
    ]
  )

  const result = []

  try {
    for await (const it of resultGen) {
      result.push(...it)
    }
  } catch (err) {
    console.log(err)
    throw err
  }

  assert.deepEqual(result, [
    ['code', 'name'],
    ['one', 'foo 1'],
    ['tow', 'foo 2'],
    ['three', 'foo 3'],
    ['one', 'bar 1'],
    ['one', 'baz 1'],
    ['tow', 'bar 2'],
    ['tow', 'baz 2'],
    ['three', 'bar 3'],
    ['three', 'baz 3']
  ])
})

test('transforms:mergeFork #2', async () => {
  const transformConfig: TableTransfromConfig = {
    transforms: [
      tf.mergeFork({
        outputColumns: ['code', 'value', 'error_name', 'error_message'],
        transformConfigs: [
          {
            transforms: [
              tf.flatMapWithProvider({
                sourceProvider: csvSourceProvider,
                outputColumns: ['code', 'value'],
                transformConfig: {
                  inputHeader: {
                    mode: 'EXCEL_STYLE'
                  },
                  transforms: [
                    tf.column.add({ columnName: 'code' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'INCLUDES',
                      testValue: 'Код',
                      range: 'A2:E7',
                      targetColumn: 'code'
                    }),

                    tf.column.add({ columnName: 'value' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'STARTS_WITH',
                      testValue: 'Значение',
                      range: 'A2:E7',
                      targetColumn: 'value'
                    }),

                    tf.column.filter({
                      columnName: 'code',
                      expression: 'not empty(value())'
                    })
                  ],
                  errorHandle: {
                    errorColumn: 'error',
                    outputColumns: ['error_name', 'error_message'],
                    transforms: [
                      tf.column.add({ columnName: 'error_name' }),
                      tf.column.transform({
                        columnName: 'error_name',
                        expression: `name of 'error'`
                      }),

                      tf.column.add({ columnName: 'error_message' }),
                      tf.column.transform({
                        columnName: 'error_message',
                        expression: `message of 'error'`
                      })
                    ]
                  }
                }
              })
            ]
          },
          {
            transforms: [
              tf.flatMapWithProvider({
                sourceProvider: csvSourceProvider,
                outputColumns: ['code', 'value'],
                transformConfig: {
                  inputHeader: {
                    mode: 'EXCEL_STYLE'
                  },
                  transforms: [
                    tf.column.add({ columnName: 'code' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'EQUAL',
                      testValue: 'Code',
                      range: 'A1:F3',
                      targetColumn: 'code'
                    }),

                    tf.column.add({ columnName: 'value' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testValue: 'Value',
                      range: 'A1:F3',
                      targetColumn: 'value'
                    }),

                    tf.column.filter({
                      columnName: 'code',
                      expression: 'not empty(value())'
                    })
                  ],
                  errorHandle: {
                    errorColumn: 'error',
                    outputColumns: ['error_name', 'error_message'],
                    transforms: [
                      tf.column.transform({
                        columnName: 'error_name',
                        expression: `name of 'error'`
                      }),
                      tf.column.transform({
                        columnName: 'error_message',
                        expression: `message of 'error'`
                      })
                    ]
                  }
                }
              })
            ]
          }
        ]
      })
    ]
  }

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/flatMapWithProvider/case2/source.csv'
      ),
      'utf8'
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 10 }),

    createTableTransformer(transformConfig),

    new FlattenTransform()
  )

  const transformedRows = []

  for await (const chunk of transformedRowsStream) {
    transformedRows.push(chunk)
  }

  assert.equal(transformedRows.length, 11)

  assert.equal(transformedRows[0][0], 'code')

  transformedRows.shift()

  assert.equal(
    transformedRows.filter((row: any) => row[2] === 'Error').length,
    2
  )

  const codes = transformedRows
    .filter((row: any) => row[0] !== null)
    .map(row => row[0])
    .sort()

  assert.deepEqual(codes, [
    '1001',
    '1002',
    '1003',
    '1004',
    '2001',
    '2002',
    '2003',
    '2004'
  ])
})
