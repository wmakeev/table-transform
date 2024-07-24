import { parse } from 'csv-parse'
import assert from 'node:assert/strict'
import { createReadStream } from 'node:fs'
import path from 'node:path'
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

test('No-opt transform #1', async () => {
  const tableTransformer = createTableTransformer({
    transforms: []
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case1.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 100 }),

    tableTransformer,

    new FlattenTransform()
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

test('No-opt transform #2', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [async chunkInfo => chunkInfo]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case1.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 100 }),

    tableTransformer,

    new FlattenTransform()
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

test('column transform (basic) #1', async () => {
  const tableTransformer = createTableTransformer({
    transforms: [
      transforms.column.transform({
        columnName: 'Наименование',
        expression: `'Бренд' & " " & 'Наименование'`
      })
    ]
  })

  const transformedRowsStream: Readable = compose(
    createReadStream(path.join(process.cwd(), 'test/cases/case1.csv'), {
      highWaterMark: 16 * 1024,
      encoding: 'utf8'
    }),

    parse({ bom: true }),

    new ChunkTransform({ batchSize: 100 }),

    tableTransformer,

    new FlattenTransform()
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
