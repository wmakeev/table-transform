import test from 'node:test'
import {
  TableRow,
  createTableTransformer,
  transforms as tf
} from '../../src/index.js'
import assert from 'node:assert/strict'
import { createTransformedRowsStreamFromCsv } from '../helpers/index.js'
import { NonExistColumnTransformError } from '../../src/errors/index.js'

test('Transform error handler #1', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'foo',
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
  assert.equal((col3 as any).name, 'Error')

  // 3
  nextResult = await gen.next()

  assert.equal(nextResult.done, true)
  assert.deepEqual(nextResult.value, undefined)
})

test('Transform error handler #2', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'foo',
        expression: `if 'bar' == 42 then 'baz' else 'foo' & "+"`
      })
    ],
    errorHandle: {
      errorColumn: 'error',
      transforms: [
        tf.column.transform({
          columnName: 'error',
          expression: `message of 'error'`
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
    [['foo', 'bar', 'error']],
    [['тест 0+', 40, null]],
    [[null, null, 'Accessing a non-existing column - "baz"']]
  ])
})

test('Transform error handler #3', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'Код',
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
    assert.ok(err instanceof NonExistColumnTransformError)
    assert.equal(err.columnName, 'foo')
  }

  assert.ok(result.length > 0)
})

test('Transform error handler #4', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'Код',
        expression: `if value() == "AG01153" then 'foo' else value()`
      })
    ],
    errorHandle: {
      errorColumn: 'error',
      transforms: [
        tf.column.transform({
          columnName: 'error',
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

  assert.ok(result.length > 0)

  const errorRowWithHeader = [result[0], result.at(-1)]

  // #dhf042pf
  assert.deepEqual(errorRowWithHeader, [
    ['Код', 'Бренд', 'Наименование', 'Закупочная цена', 'Остаток', 'error'],
    [null, null, null, null, null, 'Accessing a non-existing column - "foo"']
  ])
})

test('Transform error handler #5', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'Код',
        expression: `if value() == "1211IX352WDRH" then 'foo' else value()`
      })
    ],
    errorHandle: {
      errorColumn: 'error',
      outputColumns: ['error_name', 'error_message'],
      transforms: [
        tf.column.transform({
          columnName: 'error_name',
          expression: `name of 'error'`
        }),
        tf.column.transform({
          columnName: 'error_message',
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

  assert.ok(result.length > 0)

  const errorRowWithHeader = [result[0], result.at(-1)]

  // #dhf042pf
  assert.deepEqual(errorRowWithHeader, [
    [
      'Код',
      'Бренд',
      'Наименование',
      'Закупочная цена',
      'Остаток',
      'error_name',
      'error_message'
    ],
    [
      null,
      null,
      null,
      null,
      null,
      'Error',
      'Accessing a non-existing column - "foo"'
    ]
  ])
})

test('Transform error handler #6', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      tf.column.transform({
        columnName: 'Код',
        expression: `if value() == "1211IX352WDRH" then 'foo' else value()`
      })
    ],
    errorHandle: {
      errorColumn: 'error',
      transforms: [
        tf.column.transform({
          columnName: 'error',
          expression: `bar of 'error'`
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
    assert.equal(err.message, 'Property “bar” does not exist.')
  }

  assert.equal(result.length, 900)

  // #dhf042pf
  assert.deepEqual(result[0], [
    'Код',
    'Бренд',
    'Наименование',
    'Закупочная цена',
    'Остаток',
    'error'
  ])
})
