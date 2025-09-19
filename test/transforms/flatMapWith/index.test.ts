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
  TableRowFlatMapper,
  TableTransformConfig,
  createTableTransformer,
  transforms as tf
} from '../../../src/index.js'
import { createTestContext } from '../../_common/TestContext.js'
import { csvSourceFlatMapper } from '../../helpers/index.js'

test('transforms:flatMapWith (case1)', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.rename({
        oldColumn: 'Path to file',
        newColumn: 'file_path'
      }),

      tf.flatMapWith({
        mapper: csvSourceFlatMapper,
        outputColumns: ['code', 'name', 'value']
      })
    ]
  }

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(process.cwd(), 'test/transforms/flatMapWith/case1/source.csv'),
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

test('transforms:flatMapWith (case1) - passThroughColumns', async () => {
  const tableTransformConfig: TableTransformConfig = {
    transforms: [
      tf.column.rename({
        oldColumn: 'Path to file',
        newColumn: 'file_path'
      }),

      tf.flatMapWith({
        mapper: csvSourceFlatMapper,
        passThroughColumns: ['Name'],
        outputColumns: ['case_name', 'code', 'name', 'value'],
        transformConfig: {
          transforms: [
            tf.column.rename({
              oldColumn: 'Name',
              newColumn: 'case_name'
            })
          ]
        }
      })
    ]
  }

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(process.cwd(), 'test/transforms/flatMapWith/case1/source.csv'),
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
    ['case_name', 'code', 'name'  , 'value'   ],
    ['case1'    , '1'   , 'name-1', 'value-01'],
    ['case1'    , '2'   , 'name-2', 'value-02'],
    ['case1'    , ''    , ''      , '(none)'  ],
    ['case1'    , '3'   , 'name-3', 'value-03'],
    ['case1'    , '4'   , 'name-4', 'value-04'],
    ['case1'    , '5'   , 'name-5', 'value-05'],
    ['case2'    , '6'   , 'name-6', 'value-06'],
    ['case2'    , '7'   , 'name-7', 'value-07'],
    ['case2'    , '8'   , ''      , 'value-08'],
    ['case2'    , '9'   , 'name-9', 'value-09']
  ])
})

test('transforms:flatMapWith (case2)', async () => {
  const tableTransformConfig: TableTransformConfig = {
    context: createTestContext(),
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
  }

  const transformedRowsStream: Readable = compose(
    createReadStream(
      path.join(process.cwd(), 'test/transforms/flatMapWith/case2/source.csv'),
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
    [
      null,
      null,
      'TransformStepChunkError',
      'Cell "Код" in "A2:E7" range not found'
    ]
  ])
})

test('transforms:flatMapWith (error handle)', async () => {
  let sourceRowIndex = 0

  const flatMapperWithErrors: TableRowFlatMapper = async function* (
    header,
    row
  ) {
    const rowIndex = sourceRowIndex++

    if (rowIndex === 1) {
      throw new Error('Error №1 in mapper')
    }

    const resultHeader = header.map(h => h.name)

    yield [resultHeader]

    for (let i = 0; i < 3; i++) {
      if ((rowIndex === 3 && i === 2) || (rowIndex === 0 && i === 1)) {
        throw new Error('Error №2 in mapper')
      }
      yield [[...row]]
    }
  }

  const tableTransformConfig: TableTransformConfig = {
    context: createTestContext(),
    transforms: [
      tf.flatMapWith({
        mapper: flatMapperWithErrors,
        outputColumns: ['code', 'value', 'error_message'],
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

            tf.column.rename({ oldColumn: 'col1', newColumn: 'code' }),
            tf.column.rename({ oldColumn: 'col2', newColumn: 'value' })
          ],
          errorHandle: {
            errorColumn: 'error',
            transforms: [
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

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['code', 'value'   , 'error_message'     ],
    ['1'   , 'col2-val', null                ],
    [null  , null      , 'Error №2 in mapper'],
    [null  , null      , 'Error №1 in mapper'],
    ['3'   , 'col2-val', null                ],
    ['3'   , 'col2-val', null                ],
    ['3'   , 'col2-val', null                ],
    ['4'   , 'col2-val', null                ],
    ['4'   , 'col2-val', null                ],
    [null  , null      , 'Error №2 in mapper']
  ])
})
