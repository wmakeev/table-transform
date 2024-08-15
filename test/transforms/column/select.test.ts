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
  transforms as tf
} from '../../../src/index.js'

test('transform:column:select', async t => {
  await t.test('simple', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        tf.column.select({
          columns: ['B']
        })
      ]
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

  await t.test('error', async () => {
    const tableTransformer = createTableTransformer({
      inputHeader: {
        mode: 'EXCEL_STYLE'
      },
      transforms: [
        tf.column.select({
          columns: ['B', 'X']
        })
      ]
    })

    const csv = [['', '', '']]

    const transformedRowsStream: Readable = compose(
      csv.values(),
      new ChunkTransform(),
      tableTransformer,
      new FlattenTransform()
    )

    try {
      await transformedRowsStream.toArray()
      assert.fail('should throw error')
    } catch (err) {
      assert.ok(err instanceof Error)
      assert.equal(
        err.message,
        `Column(s) not found and can't be selected: "X"`
      )
    }
  })

  await t.test('multicolumn', async () => {
    const tableTransformer = createTableTransformer({
      transforms: [
        tf.column.select({
          columns: ['B', 'A', 'A', 'X', 'C'],
          addMissingColumns: true
        }),
        tf.tapHeader({
          tapFunction(header) {
            assert.deepEqual(header, [
              {
                index: 1,
                name: 'B',
                isDeleted: false
              },
              {
                index: 0,
                name: 'A',
                isDeleted: false
              },
              {
                index: 4,
                name: 'A',
                isDeleted: false
              },
              {
                index: 5,
                name: 'X',
                isDeleted: false
              },
              {
                index: 3,
                name: 'C',
                isDeleted: false
              },
              {
                index: 2,
                name: 'B',
                isDeleted: true
              }
            ])
          }
        })
      ]
    })

    /* prettier-ignore */
    const csv = [
      ['A' , 'B' , 'B' , 'C' , 'A' ],
      ['1' , ''  , ''  , ''  , ''  ],
      ['a1', 'b1', 'b2', 'c1', 'a2'],
      [''  , ''  , ''  , ''  , '2' ]
    ]

    const transformedRowsStream: Readable = compose(
      csv.values(),

      new ChunkTransform({ batchSize: 2 }),

      tableTransformer,

      new FlattenTransform()
    )

    const transformedRows = await transformedRowsStream.toArray()

    /* prettier-ignore */
    assert.deepEqual(transformedRows, [
      ['B' , 'A' , 'A' , 'X' , 'C' ],
      [''  , '1' , ''  , null, ''  ],
      ['b1', 'a1', 'a2', null, 'c1'],
      [''  , ''  , '2' , null, ''  ]
    ])
  })
})
