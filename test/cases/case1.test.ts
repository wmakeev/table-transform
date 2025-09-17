import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import test from 'node:test'
import { createTableTransformer, transforms } from '../../src/index.js'
import { createTestContext } from '../_common/TestContext.js'
import { createTransformedRowsStreamFromCsv } from '../helpers/index.js'

test('No-opt transform #1', async () => {
  const tableTransformer = createTableTransformer({
    transforms: []
  })

  const transformedRowsStream: Readable = createTransformedRowsStreamFromCsv(
    'test/cases/case1.csv',
    tableTransformer,
    { batchSize: 100 }
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.equal(transformedRows.length, 25747)

  const pick = transformedRows.slice(0, 3)

  assert.deepEqual(pick, [
    ['Код', 'Бренд', 'Наименование', 'Закупочная цена', 'Остаток'],
    ['2122FOCIII', 'Febest', 'Тяга рулевая', '986.00', '1'],
    ['1782A3RA43', 'Febest', 'Ступица задняя', '2836.00', '1']
  ])
})

test('No-opt transform (skip header)', async () => {
  const tableTransformer = createTableTransformer({
    outputHeader: {
      skip: true
    },
    transforms: []
  })

  const transformedRowsStream: Readable = createTransformedRowsStreamFromCsv(
    'test/cases/case1.csv',
    tableTransformer,
    { batchSize: 1000 }
  )

  const transformedRows = await transformedRowsStream.take(3).toArray()

  assert.equal(transformedRows.length, 3)

  assert.deepEqual(transformedRows, [
    ['2122FOCIII', 'Febest', 'Тяга рулевая', '986.00', '1'],
    ['1782A3RA43', 'Febest', 'Ступица задняя', '2836.00', '1'],
    ['052382', 'Gates', 'Шланг системы охлаждения', '1852.01', '10']
  ])
})

test('No-opt transform #3', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [chunkInfo => chunkInfo]
  })

  const transformedRowsStream: Readable = createTransformedRowsStreamFromCsv(
    'test/cases/case1.csv',
    tableTransformer,
    { batchSize: 100 }
  )

  const transformedRows = await transformedRowsStream.toArray()

  assert.equal(transformedRows.length, 25747)

  const pick = transformedRows.slice(0, 3)

  assert.deepEqual(pick, [
    ['Код', 'Бренд', 'Наименование', 'Закупочная цена', 'Остаток'],
    ['2122FOCIII', 'Febest', 'Тяга рулевая', '986.00', '1'],
    ['1782A3RA43', 'Febest', 'Ступица задняя', '2836.00', '1']
  ])
})

test('Column transform (basic) #1', async () => {
  const tableTransformer = createTableTransformer({
    context: createTestContext(),
    transforms: [
      transforms.column.transform({
        column: 'Наименование',
        expression: `'Бренд' & " " & 'Наименование'`
      })
    ]
  })

  const transformedRowsStream: Readable = createTransformedRowsStreamFromCsv(
    'test/cases/case1.csv',
    tableTransformer,
    { batchSize: 100 }
  )

  const transformedRows = await transformedRowsStream.take(5).toArray()

  assert.deepEqual(transformedRows, [
    ['Код', 'Бренд', 'Наименование', 'Закупочная цена', 'Остаток'],
    ['2122FOCIII', 'Febest', 'Febest Тяга рулевая', '986.00', '1'],
    ['1782A3RA43', 'Febest', 'Febest Ступица задняя', '2836.00', '1'],
    ['052382', 'Gates', 'Gates Шланг системы охлаждения', '1852.01', '10'],
    ['0523TRBF', 'Febest', 'Febest Тяга стабилизатора передняя', '624.00', '1']
  ])
})
