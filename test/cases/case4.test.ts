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
  createTableTransformer
} from '../../src/index.js'

test('Empty headers #4 (error)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: []
  })

  const transformedRowsStream: Readable = compose(
    [[], [1, 2, 3]].values(),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('Error expected')
  } catch (err) {
    assert.ok(err instanceof Error)
    assert.equal(err.message, 'Source have not any header')
  }
})

test('Empty headers #4 (correct)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: []
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case4.csv'), 'utf8'),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.equal(transformedRows.length, 3)

  assert.deepEqual(transformedRows, [
    ['', '', '', '', '', ''],
    ['', 'foo', '', 'bar', '', ''],
    ['', '', '', '', '', 'baz']
  ])
})
