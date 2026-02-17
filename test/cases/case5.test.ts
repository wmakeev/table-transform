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
  transforms as tf
} from '../../src/index.js'

test('Trim headers #5', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'FIRST_ROW',
      trimHeaderNames: true
    },
    transforms: [
      tf.header.assert({
        headers: [
          '№',
          'Name',
          'Value',
          'List',
          'List',
          'Some',
          'List',
          'Num',
          'Foo',
          'Bar'
        ]
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case5.csv'), 'utf8'),
    parse({ bom: true }),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.equal(transformedRows.length, 3)

  assert.deepEqual(transformedRows, [
    ['№', 'Name', 'Value', 'List', 'List', 'Some', 'List', 'Num', 'Foo', 'Bar'],
    ['1', 'String', 'Some text', 'One', '2', '', '3️⃣', '10', '1', ''],
    ['2', 'Строка', 'Просто текст', '', '', '', '', '20', '', 'a']
  ])
})
