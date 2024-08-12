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
  SourceProvider,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { csvSourceProvider } from '../../helpers/index.js'

test('transforms:flatMapWithProvider (case1)', async () => {
  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.column.rename({
        oldColumnName: 'Path to file',
        newColumnName: 'file_path'
      }),

      tf.flatMapWithProvider({
        sourceProvider: csvSourceProvider,
        outputColumns: ['code', 'name', 'value']
      })
    ]
  }

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(
        process.cwd(),
        'test/transforms/flatMapWithProvider/case1/source.csv'
      ),
      'utf8'
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 10 }),

    createTableTransformer(tableTransformConfig),

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['code', 'name'  , 'value'   ],
    ['1'   , 'name-1', 'value-01'],
    ['2'   , 'name-2', 'value-02'],
    [''    , ''      , '(none)'  ],
    ['3'   , 'name-3', 'value-03'],
    ['4'   , 'name-4', 'value-04'],
    ['5'   , 'name-5', 'value-05'],
    ['6'   , 'name-6', 'value-06'],
    ['7'   , 'name-7', 'value-07'],
    ['8'   , ''      , 'value-08'],
    ['9'   , 'name-9', 'value-09']
  ])
})

test('transforms:flatMapWithProvider (case2)', async () => {
  const tableTransformConfig: TableTransfromConfig = {
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

    createTableTransformer(tableTransformConfig),

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* _prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['code', 'value', 'error_name', 'error_message'],
    ['1001', '11', null, null],
    ['1002', '12', null, null],
    ['1003', '', null, null],
    ['1004', '14', null, null],
    [null, null, 'Error', 'Cell "Код" in "A2:E7" range not found']
  ])
})

test('transforms:flatMapWithProvider (error handle)', async () => {
  let sourceRowIndex = 0

  const sourceProviderWithErrors: SourceProvider = async function* (
    header,
    row
  ) {
    const rowIndex = sourceRowIndex++

    if (rowIndex === 1) {
      throw new Error('Error №1 in SourceProvider')
    }

    const resultHeader = header.map(h => h.name)

    yield [resultHeader]

    for (let i = 0; i < 3; i++) {
      if ((rowIndex === 3 && i === 2) || (rowIndex === 0 && i === 1)) {
        throw new Error('Error №2 in SourceProvider')
      }
      yield [[...row]]
    }
  }

  const tableTransformConfig: TableTransfromConfig = {
    transforms: [
      tf.flatMapWithProvider({
        sourceProvider: sourceProviderWithErrors,
        outputColumns: ['code', 'value'],
        transformConfig: {
          transforms: [
            tf.tapHeader({
              tapFunction(header) {
                assert.ok(header)
              }
            }),
            tf.tapRows({
              tapFunction(chunk, header) {
                assert.ok(chunk)
                assert.ok(header)
              }
            }),

            tf.column.rename({ oldColumnName: 'col1', newColumnName: 'code' }),
            tf.column.rename({ oldColumnName: 'col2', newColumnName: 'value' })
          ],
          errorHandle: {
            errorColumn: 'error',
            outputColumns: ['error_message'],
            transforms: [
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
  }

  const transformedRowsStream: Readable = compose(
    [
      [
        ['col1', 'col2'],
        ['1', 'col2-val'],
        ['2', 'col2-val']
      ],
      [
        ['3', 'col2-val'],
        ['4', 'col2-val']
      ]
    ].values(),

    createTableTransformer(tableTransformConfig),

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* _prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['code', 'value', 'error_message'],
    ['1', 'col2-val', null],
    [null, null, 'Error №2 in SourceProvider'],
    [null, null, 'Error №1 in SourceProvider'],
    ['3', 'col2-val', null],
    ['3', 'col2-val', null],
    ['3', 'col2-val', null],
    ['4', 'col2-val', null],
    ['4', 'col2-val', null],
    [null, null, 'Error №2 in SourceProvider']
  ])
})
