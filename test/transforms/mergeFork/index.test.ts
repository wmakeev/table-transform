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
import { csvSourceFlatMapper } from '../../helpers/index.js'

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

              tf.column.add({ column: 'code', defaultValue: 'one' }),
              tf.column.transform({
                column: 'name',
                expression: 'value() & " 1"'
              })
            ]
          },
          {
            transforms: [
              tf.column.add({ column: 'code', defaultValue: 'tow' }),
              tf.column.transform({
                column: 'name',
                expression: 'value() & " 2"'
              })
            ]
          },
          {
            transforms: [
              tf.column.add({ column: 'code', defaultValue: 'three' }),
              tf.column.transform({
                column: 'name',
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

  assert.deepEqual(result.shift(), ['code', 'name'])

  result.sort()

  assert.deepEqual(result, [
    ['one', 'bar 1'],
    ['one', 'baz 1'],
    ['one', 'foo 1'],
    ['three', 'bar 3'],
    ['three', 'baz 3'],
    ['three', 'foo 3'],
    ['tow', 'bar 2'],
    ['tow', 'baz 2'],
    ['tow', 'foo 2']
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
              tf.flatMapWith({
                mapper: csvSourceFlatMapper,
                outputColumns: ['code', 'value', 'error_name', 'error_message'],
                transformConfig: {
                  inputHeader: {
                    mode: 'EXCEL_STYLE'
                  },
                  transforms: [
                    tf.column.add({ column: 'code' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'INCLUDES',
                      testValue: 'Код',
                      range: 'A2:E7',
                      targetColumn: 'code'
                    }),

                    tf.column.add({ column: 'value' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'STARTS_WITH',
                      testValue: 'Значение',
                      range: 'A2:E7',
                      targetColumn: 'value'
                    }),

                    tf.column.filter({
                      column: 'code',
                      expression: 'not empty(value())'
                    })
                  ],
                  errorHandle: {
                    errorColumn: 'error',
                    transforms: [
                      tf.column.add({ column: 'error_name' }),
                      tf.column.transform({
                        column: 'error_name',
                        expression: `name of 'error'`
                      }),

                      tf.column.add({ column: 'error_message' }),
                      tf.column.transform({
                        column: 'error_message',
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
              tf.flatMapWith({
                mapper: csvSourceFlatMapper,
                outputColumns: ['code', 'value', 'error_name', 'error_message'],
                transformConfig: {
                  inputHeader: {
                    mode: 'EXCEL_STYLE'
                  },
                  transforms: [
                    tf.column.add({ column: 'code' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testOperation: 'EQUAL',
                      testValue: 'Code',
                      range: 'A1:F3',
                      targetColumn: 'code'
                    }),

                    tf.column.add({ column: 'value' }),
                    tf.column.sheetCell({
                      type: 'HEADER',
                      testValue: 'Value',
                      range: 'A1:F3',
                      targetColumn: 'value'
                    }),

                    tf.column.filter({
                      column: 'code',
                      expression: 'not empty(value())'
                    })
                  ],
                  errorHandle: {
                    errorColumn: 'error',
                    transforms: [
                      tf.column.add({ column: 'error_name' }),
                      tf.column.transform({
                        column: 'error_name',
                        expression: `name of 'error'`
                      }),

                      tf.column.add({ column: 'error_message' }),
                      tf.column.transform({
                        column: 'error_message',
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
      path.join(process.cwd(), 'test/transforms/flatMapWith/case2/source.csv'),
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
    transformedRows.filter((row: any) => {
      return row[2]?.includes('Error')
    }).length,
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
