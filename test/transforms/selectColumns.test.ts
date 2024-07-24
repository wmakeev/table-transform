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
  transforms
} from '../../src/index.js'

test('select header transform', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.select({
        columns: ['B']
      })
    ],
    prependHeaders: 'EXCEL_STYLE'
  })

  const csv = [
    ['', '', ''],
    ['', '1', ''],
    ['', '', ''],
    ['', '2', '']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),

    new ChunkTransform({ batchSize: 2 }),

    tableTransformer,

    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.deepEqual(transformedRows, [['B'], [''], ['1'], [''], ['2']])
})
