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
  createTableTransformer,
  transforms
} from '../../src/index.js'

test('column transform (array) #2', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.transform({
        columnName: 'List',
        expression: `row() & ":" & arrayIndex() & if empty(value()) then "" else " - " & value()`
      }),

      transforms.column.transform({
        columnName: 'Some',
        expression: `if row() == 3 then column() else value()`
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case2.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 100 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.take(5).toArray()

  /* prettier-ignore */
  assert.deepEqual(
    transformedRows,
    [
      ['№', 'Name'   , 'Value'           , 'List'      , 'List'   , 'Some'   , 'List'    , 'Num'],
      ['1', 'String' , 'Some text'       , '1:0 - One' , '1:1 - 2', ''       , '1:2 - 3️⃣', '10' ],
      ['2', 'Строка' , 'Просто текст'    , '2:0'       , '2:1'    , ''       , '2:2'     , '20' ],
      ['3', 'Emoji'  , '😀 Text 💯 Текст', '3:0'       , '3:1'    , 'Some'   , '3:2'     , '30' ],
      ['4', 'Integer', '1000'            , '4:0'       , '4:1'    , ''       , '4:2'     , '15' ]
    ]
  )
})

test('column transform (values)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.add({
        columnName: 'Value1'
      }),

      transforms.column.transform({
        columnName: 'Value1',
        expression: `value("Num")`
      }),

      transforms.column.add({
        columnName: 'Value2'
      }),

      transforms.column.transform({
        columnName: 'Value2',
        expression: `value("List")`
      }),

      transforms.column.add({
        columnName: 'Values1'
      }),

      transforms.column.add({
        columnName: 'Values2'
      }),

      transforms.column.transform({
        columnName: 'Values1',
        expression: `values("Num")`
      }),

      transforms.column.transform({
        columnName: 'Values2',
        expression: `values("List")`
      }),

      transforms.column.select({
        columns: ['Value1', 'Value2', 'Values1', 'Values2']
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case2.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 100 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.take(5).toArray()

  assert.deepEqual(
    transformedRows,
    /* prettier-ignore */
    [
      ['Value1', 'Value2', 'Values1'  , 'Values2'        ],
      ['10'    , 'One'   , ['10']     , ['One','2', '3️⃣']],
      ['20'    , ''      , ['20']     , ['', '', '']     ],
      ['30'    , ''      , ['30']     , ['', '', '']     ],
      ['15'    , ''      , ['15']     , ['', '', '']     ]
    ]
  )
})
