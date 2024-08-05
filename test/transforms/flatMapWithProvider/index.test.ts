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
