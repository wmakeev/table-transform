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
  transforms as tf
} from '../../../src/index.js'
import { createReadStream } from 'node:fs'
import { parse } from 'csv-parse'
import path from 'node:path'

test('flatMapWith transform', async () => {
  const sourceProvider = async function* (filePath: unknown) {
    if (typeof filePath !== 'string') {
      throw new Error('filePath is not string')
    }

    const sourceStream: Readable = compose(
      createReadStream(path.join(process.cwd(), filePath), 'utf8'),
      parse({ bom: true }),
      new ChunkTransform({ batchSize: 2 })
    )

    yield* sourceStream
  }

  const rootTransformer = createTableTransformer({
    transforms: [
      tf.flatMapWith({
        sourceRefColumn: 'file_uri',
        sourceProvider,
        outputHeaders: ['code', 'value'],
        transformSelection: 'PASSED',
        transformConfigs: [
          {
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
              })
            ],
            prependHeaders: 'EXCEL_STYLE'
          },
          {
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
              })
            ],
            prependHeaders: 'EXCEL_STYLE'
          }
        ]
      }),

      tf.column.filter({
        columnName: 'code',
        expression: 'not empty(value())'
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(process.cwd(), 'test/transforms/flatMapWith/source.csv'),
      {
        highWaterMark: 16 * 1024,
        encoding: 'utf8'
      }
    ),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    rootTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [
    ['code', 'value'],
    ['1001', '11'],
    ['1002', '12'],
    ['1003', ''],
    ['1004', '14'],
    ['2001', '21'],
    ['2002', '22'],
    ['2003', '23'],
    ['2004', '24']
  ])
})
