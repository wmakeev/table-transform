import assert from 'node:assert'
import { Readable } from 'node:stream'
import test from 'node:test'
import { ChunkTransform, FlattenTransform } from '../../../src/index.js'

test('ChunkTransform', async () => {
  const items = [...Array(1000).fill('').keys()]

  const stream = Readable.from(items).pipe(
    new ChunkTransform({ batchSize: 120 })
  )

  const result = await stream.toArray()

  assert.ok(result)
  assert.equal(result.length, 9)
  assert.equal(result[0].length, 120)
  assert.equal(result[8].length, 40)
})

test('FlattenTransform', async () => {
  const items = [...Array(1000).fill('').keys()]

  const stream = Readable.from(items)
    .pipe(new ChunkTransform({ batchSize: 120 }))
    .pipe(new FlattenTransform())

  const result = await stream.toArray()

  assert.ok(result)
  assert.equal(result.length, 1000)
  assert.equal(result[0], 0)
  assert.equal(result[999], 999)
})
