import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose,
  Readable
} from 'node:stream'
import test from 'node:test'
import { TransformStepRowAssertError } from '../../../src/errors/index.js'
import {
  ChunkTransform,
  createTableTransformer,
  FlattenTransform,
  transforms
} from '../../../src/index.js'
import { createTestContext } from '../../_common/TestContext.js'

test('transforms:column:assert', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.assert({
        message: 'expected B to be equal "foo"',
        column: 'B',
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
    assert.ok(err instanceof TransformStepRowAssertError)
    assert.equal(err.message, 'expected B to be equal "foo"')
    assert.equal(err.rowNum, 3)
    err.report()
  }
})

test('transforms:column:assert (array with selected index)', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      transforms.column.assert({
        column: 'A',
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
