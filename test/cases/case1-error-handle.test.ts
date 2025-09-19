import assert from 'node:assert/strict'
import test from 'node:test'
import {
  TransformStepColumnsNotFoundError,
  TransformStepRowExpressionError
} from '../../src/errors/index.js'
import {
  TableRow,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import { createTestContext } from '../_common/TestContext.js'
import { createTransformedRowsStreamFromCsv } from '../helpers/index.js'

test('Transform error handler', async t => {
  await t.test('place error name in created column', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.add({ column: 'error' }),
        tf.column.transform({
          column: 'foo',
          expression: `'baz'`
        })
      ],
      errorHandle: {
        errorColumn: 'error'
      }
    })

    const gen = tableTransformer([
      [
        ['foo', 'bar'],
        ['тест', 42]
      ]
    ])

    let nextResult

    // 1
    nextResult = await gen.next()

    assert.equal(nextResult.done, false)
    assert.deepEqual(nextResult.value, [['foo', 'bar', 'error']])

    // 2
    nextResult = await gen.next()

    assert.equal(nextResult.done, false)
    assert.ok(Array.isArray(nextResult.value))
    assert.ok(nextResult.value[0])
    const [col1, col2, col3] = nextResult.value[0]

    assert.equal(col1, null)
    assert.equal(col2, null)
    assert.equal((col3 as any).name, 'TransformStepRowExpressionError')

    // 3
    nextResult = await gen.next()

    assert.equal(nextResult.done, true)
    assert.deepEqual(nextResult.value, undefined)
  })

  await t.test('place error name in created column', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'foo',
          expression: `'baz'`
        })
      ],
      errorHandle: {
        errorColumn: 'error'
      }
    })

    const result = []

    for await (const chunk of tableTransformer([
      [
        ['foo', 'bar'],
        ['тест', 42],
        ['тест', 43]
      ]
    ])) {
      result.push(chunk)
    }

    assert.deepEqual(result, [[['foo', 'bar']], [[null, null]]])
  })

  await t.test(
    'partially processed with error handler and forced columns',
    async () => {
      const tableTransformer = createTableTransformer({
        context: createTestContext(),
        outputHeader: {
          forceColumns: ['foo', 'bar', 'error_message', 'error_foo']
        },
        transforms: [
          tf.column.transform({
            column: 'foo',
            expression: `if 'bar' == 42 then 'baz' else 'foo' & "+"`
          })
        ],
        errorHandle: {
          errorColumn: 'error',
          transforms: [
            tf.column.add({ column: 'error_message' }),
            tf.column.transform({
              column: 'error_message',
              expression: `message of 'error'`
            }),

            tf.column.add({ column: 'error_foo' }),
            tf.column.transform({
              column: 'error_foo',
              expression: `foo of rowRecord of 'error'`
            })
          ]
        }
      })

      const gen = tableTransformer([
        [
          ['foo', 'bar'],
          ['тест 0', 40]
        ],
        [
          ['тест 1', 41],
          ['тест 2', 42],
          ['тест 3', 43]
        ]
      ])

      const result = []

      for await (const it of gen) result.push(it)

      assert.deepEqual(result, [
        [['foo', 'bar', 'error_message', 'error_foo']],
        [['тест 0+', 40, null, null]],
        [[null, null, 'Column(s) not found: "baz"', 'тест 2']]
      ])
    }
  )

  await t.test('partially processed without error handler', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.transform({
          column: 'Код',
          expression: `if value() == "AG01153" then 'foo' else value()`
        })
      ]
    })

    const transformedRowsStream = createTransformedRowsStreamFromCsv(
      'test/cases/case1.csv',
      tableTransformer,
      { batchSize: 1000 }
    )

    const result = []

    try {
      for await (const it of transformedRowsStream) {
        result.push(it as TableRow)
      }

      assert.fail('error expected')
    } catch (err) {
      assert.ok(err instanceof TransformStepRowExpressionError)
      assert.ok(err.cause instanceof TransformStepColumnsNotFoundError)
      assert.deepEqual(err.cause.columns, ['foo'])
      err.report()
    }

    assert.ok(result.length > 0)
  })

  await t.test('partially processed large csv with error handler', async () => {
    const tableTransformer = createTableTransformer({
      context: createTestContext(),
      transforms: [
        tf.column.add({ column: 'error' }),
        tf.column.transform({
          column: 'Код',
          expression: `if value() == "AG01153" then 'foo' else value()`
        })
      ],
      errorHandle: {
        errorColumn: 'error',
        transforms: [
          tf.column.transform({
            column: 'error',
            expression: `message of 'error'`
          })
        ]
      }
    })

    const transformedRowsStream = createTransformedRowsStreamFromCsv(
      'test/cases/case1.csv',
      tableTransformer,
      { batchSize: 1000 }
    )

    const result = []

    try {
      for await (const it of transformedRowsStream) {
        result.push(it as TableRow)
      }

      assert.fail('error expected')
    } catch (err) {
      assert.ok(err instanceof Error)
      assert.equal(err.name, 'AbortError')
    }

    assert.ok(result.length > 10)

    const errorRowWithHeader = [result[0], result.at(-1)]

    // #dhf042pf
    assert.deepEqual(errorRowWithHeader, [
      ['Код', 'Бренд', 'Наименование', 'Закупочная цена', 'Остаток', 'error'],
      [null, null, null, null, null, 'Column(s) not found: "foo"']
    ])
  })

  await t.test(
    'partially processed csv with error handler transform',
    async () => {
      const tableTransformer = createTableTransformer({
        context: createTestContext(),
        transforms: [
          tf.column.add({ column: 'error_message' }),
          tf.column.transform({
            column: 'Код',
            expression: `if value() == "1211IX352WDRH" then 'foo' else value()`
          })
        ],
        errorHandle: {
          errorColumn: 'error',
          transforms: [
            tf.column.add({ column: 'error_name' }),
            tf.column.transform({
              column: 'error_name',
              expression: `name of 'error'`
            }),

            tf.column.add({ column: 'error_message' }),
            tf.column.transform({
              column: 'error_message',
              expression: `message of 'error'`
            })
          ]
        }
      })

      const transformedRowsStream = createTransformedRowsStreamFromCsv(
        'test/cases/case1.csv',
        tableTransformer,
        { batchSize: 100 }
      )

      const result = []

      try {
        for await (const it of transformedRowsStream) {
          result.push(it as TableRow)
        }

        assert.fail('error expected')
      } catch (err) {
        assert.ok(err instanceof Error)
        assert.equal(err.name, 'AbortError')
      }

      assert.ok(result.length > 10)

      const errorRowWithHeader = [result[0], result.at(-1)]

      // #dhf042pf
      assert.deepEqual(errorRowWithHeader, [
        [
          'Код',
          'Бренд',
          'Наименование',
          'Закупочная цена',
          'Остаток',
          'error_message'
        ],
        [null, null, null, null, null, 'Column(s) not found: "foo"']
      ])
    }
  )

  await t.test(
    'partially processed with error inside error handler',
    async () => {
      const tableTransformer = createTableTransformer({
        context: createTestContext(),
        outputHeader: {
          forceColumns: [
            'error',
            'Наименование',
            'Код',
            'Бренд',
            'Закупочная цена',
            'Остаток'
          ]
        },
        transforms: [
          tf.column.transform({
            column: 'Код',
            expression: `if value() == "1211IX352WDRH" then 'foo' else value()`
          })
        ],
        errorHandle: {
          errorColumn: 'error',
          transforms: [
            tf.column.add({ column: 'foo' }),
            tf.column.transform({
              column: 'foo',
              expression: `bar of 'error'`
            }),
            tf.column.transform({
              column: 'error',
              expression: `'bar'`
            })
          ]
        }
      })

      const transformedRowsStream = createTransformedRowsStreamFromCsv(
        'test/cases/case1.csv',
        tableTransformer,
        { batchSize: 100 }
      )

      const result = []

      try {
        for await (const it of transformedRowsStream) {
          result.push(it as TableRow)
        }

        assert.fail('error expected')
      } catch (err) {
        assert.ok(err instanceof TransformStepRowExpressionError)
        assert.equal(err.message, 'Column(s) not found: "bar"')
        err.report()
      }

      assert.equal(result.length, 900)

      // #dhf042pf
      assert.deepEqual(result[0], [
        'error',
        'Наименование',
        'Код',
        'Бренд',
        'Закупочная цена',
        'Остаток'
      ])
    }
  )
})

// TODO Нужно протестировать разные варианты возникновения ошибок
// на разных этапах, с заголовками-массивами
