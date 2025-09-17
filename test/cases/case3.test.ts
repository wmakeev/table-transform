import { parse } from 'csv-parse'
import decode from 'decode-html'
import assert from 'node:assert'
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
import { createTestContext } from '../_common/TestContext.js'

const decodeHtml = (val: unknown) => {
  if (typeof val === 'string') {
    return decode(val)
  }

  return val
}

test('complex transform #3', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      transforms.column.add({
        column: 'RowIndex'
      }),

      transforms.column.add({
        column: 'NewCol 1',
        defaultValue: '*'
      }),

      transforms.column.add({
        column: '_new_col 2'
      }),

      transforms.column.rename({
        oldColumn: 'Some',
        newColumn: 'Some renamed'
      }),

      transforms.column.rename({
        oldColumn: 'List',
        newColumn: 'list_arr'
      }),

      transforms.column.fill({
        column: 'Some renamed',
        value: 'filled'
      }),

      transforms.column.fill({
        column: 'list_arr',
        value: 42,
        arrIndex: 2
      }),

      transforms.column.filter({
        column: 'Num',
        expression: 'value() != "20"'
      }),

      transforms.column.map({
        column: 'Value',
        mapper: decodeHtml
      }),

      transforms.column.transform({
        column: 'RowIndex',
        expression: 'row()'
      }),

      transforms.column.transform({
        column: 'NewCol 1',
        expression: 'if row() == 3 then "X" else value()'
      }),

      transforms.column.select({
        columns: [
          '№',
          'RowIndex',
          'Name',
          'Value',
          'Some renamed',
          'NewCol 1',
          'list_arr',
          'list_arr',
          'list_arr',
          'Num'
        ]
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case3.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* prettier-ignore */
  assert.deepStrictEqual(
    transformedRows,
    [
      ['№', 'RowIndex', 'Name'   , 'Value'                  , 'Some renamed', 'NewCol 1', 'list_arr', 'list_arr', 'list_arr', 'Num'],
      ['1', 1         , 'String' , 'Some text'              , 'filled'      , '*'       , 'One'     , '2'       , 42        , '10' ],
      ['3', 2         , 'Emoji'  , '😀 Text 💯 "Текст&quot,', 'filled'      , '*'       , ''        , ''        , 42        , '30' ],
      ['4', 3         , 'Integer', '1000'                   , 'filled'      , 'X'       , ''        , ''        , 42        , '15' ],
      ['5', 4         , 'Float'  , '1225,55'                , 'filled'      , '*'       , ''        , ''        , 42        , '25' ],
      ['6', 5         , 'Date'   , '2023/12/12 14:23'       , 'filled'      , '*'       , ''        , ''        , 42        , '35' ]
    ]
  )
})
