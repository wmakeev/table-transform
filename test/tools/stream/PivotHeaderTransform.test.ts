import assert from 'node:assert'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import {
  ChunkTransform,
  PivotHeaderTransform,
  FlattenTransform
} from '../../../src/index.js'

test('PivotHeaderTransform', async () => {
  /* prettier-ignore */
  const items = [
    ['id', 'name', 'value'],
    ['id', 'col1', 'value1'],
    ['id', 'col2', 'value2'],
    ['id', 'col3', 'value3'],
    ['id', 'col4', 'value4'],
    ['id', 'col5', 'value5'],
    ['id', 'col6', 'value6'],
    ['id', 'col7', 'value7'],
  ]

  const stream = compose(
    Readable.from(items),
    new ChunkTransform({ batchSize: 3 }),
    new PivotHeaderTransform({
      keyColumn: 'name',
      valueColumn: 'value'
    }),
    new FlattenTransform()
  )

  const result = await stream.toArray()

  assert.ok(result)

  assert.deepEqual(result, [
    [
      'id',
      'name',
      'value',
      'col1',
      'col2',
      'col3',
      'col4',
      'col5',
      'col6',
      'col7'
    ],
    [
      'id',
      'col1',
      'value1',
      'value1',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined
    ],
    [
      'id',
      'col2',
      'value2',
      undefined,
      'value2',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined
    ],
    [
      'id',
      'col3',
      'value3',
      undefined,
      undefined,
      'value3',
      undefined,
      undefined,
      undefined,
      undefined
    ],
    [
      'id',
      'col4',
      'value4',
      undefined,
      undefined,
      undefined,
      'value4',
      undefined,
      undefined,
      undefined
    ],
    [
      'id',
      'col5',
      'value5',
      undefined,
      undefined,
      undefined,
      undefined,
      'value5',
      undefined,
      undefined
    ],
    [
      'id',
      'col6',
      'value6',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      'value6',
      undefined
    ],
    [
      'id',
      'col7',
      'value7',
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      'value7'
    ]
  ])
})
