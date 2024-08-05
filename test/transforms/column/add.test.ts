import assert from 'node:assert'
import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import test from 'node:test'
import { createTableTransformer, transforms } from '../../../src/index.js'

test('add header transform #1', async () => {
  const tableTransformer = createTableTransformer({
    inputHeader: {
      mode: 'EXCEL_STYLE'
    },
    transforms: [
      transforms.column.remove({
        columnName: 'A'
      }),
      transforms.column.add({
        columnName: 'C'
      }),
      transforms.column.add({
        columnName: 'D',
        defaultValue: 'D'
      }),
      transforms.column.add({
        columnName: 'C',
        defaultValue: 'C'
      }),
      transforms.column.add({
        columnName: 'C',
        defaultValue: 'C2',
        force: true
      })
    ]
  })

  const table = [
    [
      ['a', '', ''],
      ['a', '1', '']
    ],
    [
      ['a', '', ''],
      ['a', '2', '']
    ]
  ]

  const transformedRows: string[][] = await compose(() =>
    tableTransformer(table)
  ).toArray()

  assert.deepEqual(
    transformedRows,
    /* prettier-ignore */
    [
      [['D', 'B', 'C', 'C']],
      [
        ['D', '', '', 'C2'],
        ['D', '1', '', 'C2']
      ],
      [
        ['D', '', '', 'C2'],
        ['D', '2', '', 'C2']
      ]
    ]
  )
})
