import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import { createTableTransformer, transforms } from '../../../src/index.js'

test('transforms:column:addArray', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.addArray({
        column: 'A',
        length: 2,
        defaultValue: '!'
      }),
      transforms.column.addArray({
        column: 'B',
        length: 1
      }),
      transforms.column.addArray({
        column: 'C',
        length: 1,
        forceLength: true
      }),
      transforms.column.addArray({
        column: 'D',
        length: 1,
        forceLength: true
      })
    ]
  })

  const table = [
    [
      ['A', 'B', 'B', 'C', 'C', 'D'],
      ['a', 'b', 'b', 'c', 'c', 'd'],
      ['1', '1', '2', '1', '2', '1'],
      ['a', 'b', 'b', 'c', 'c', 'd']
    ],
    [
      ['a', 'b', 'b', 'c', 'c', 'd'],
      ['a', 'b', 'b', 'c', 'c', 'd']
    ]
  ]

  const transformedRows: string[][] = await compose(() =>
    tableTransformer(table)
  ).toArray()

  /* _prettier-ignore */
  assert.deepEqual(transformedRows, [
    [['A', 'B', 'B', 'C', 'D', 'A']],
    [
      ['a', 'b', 'b', 'c', 'd', '!'],
      ['1', '1', '2', '1', '1', '!'],
      ['a', 'b', 'b', 'c', 'd', '!']
    ],
    [
      ['a', 'b', 'b', 'c', 'd', '!'],
      ['a', 'b', 'b', 'c', 'd', '!']
    ]
  ])
})
