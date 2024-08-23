import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  createTableTransformer,
  FlattenTransform,
  transforms
} from '../../../src/index.js'
import { TransformRowAssertError } from '../../../src/errors/index.js'

test('transforms:column:assert', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.assert({
        message: 'expected B to be equal "foo"',
        columnName: 'B',
        expression: 'value() == "foo"'
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
      ['1' , 'foo', ''],
      ['2' , 'foo', ''],
      ['3' , 'bar', ''],
      ['4' , 'foo', '']
    ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 2 }),
    tableTransformer
  )

  try {
    await transformedRowsStream.toArray()
    assert.fail('should fail')
  } catch (err) {
    assert.ok(err instanceof TransformRowAssertError)
    assert.equal(err.message, 'expected B to be equal "foo"')
    assert.equal(err.rowNum, 3)
    err.report()
  }
})

test('transforms:column:assert (array with selected index)', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.assert({
        columnName: 'A',
        expression: 'value() != "x"',
        columnIndex: 0
      })
    ]
  })

  /* prettier-ignore */
  const csv = [
    ['A', 'B', 'A'],
    ['1', '2', '' ],
    ['' , '2', 'x'],
    ['1', '2', '' ],
    ['' , '2', '1']
  ]

  const transformedRowsStream: Readable = compose(
    csv.values(),
    new ChunkTransform({ batchSize: 10 }),
    tableTransformer,
    new FlattenTransform()
  )

  const transformedRows = await transformedRowsStream.toArray()

  /* prettier-ignore */
  assert.deepEqual(transformedRows, [
    ['A', 'B', 'A'],
    ['1', '2', ''],
    ['' , '2', 'x'],
    ['1', '2', ''],
    ['' , '2', '1']
  ])
})
