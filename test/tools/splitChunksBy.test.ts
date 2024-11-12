import assert from 'node:assert/strict'
import test from 'node:test'
import {
  Context,
  createTableHeader,
  splitChunksBy,
  TableChunksSource,
  TableRow
} from '../../src/index.js'

test('splitChunksBy', async () => {
  const DATA: TableRow[][] = [
    [[1]],
    [[1], [2]],
    [[3]],
    [[4], [4], [4], [4]],
    [[5], [5], [5]],
    [[5], [5], [6], [6], [6]],
    [[7], [7], [8], [8], [9]],
    [[10], [11], [12], [12], [13]],
    [[13], [13], [13]],
    [[13], [13], [13]],
    [[14], [14], [14]],
    [[14], [14], [14]],
    [[14], [15], [16]]
  ]

  const tableChunksAsyncIterable: TableChunksSource = {
    getContext() {
      return new Context()
    },
    getHeader() {
      return createTableHeader(['foo'])
    },
    async *[Symbol.asyncIterator]() {
      yield* DATA
    }
  }

  const splitedChunks: AsyncGenerator<
    [isGroupFistChunk: boolean, chunk: TableRow[]]
  > = splitChunksBy(tableChunksAsyncIterable, ['foo'])

  const groups = []

  for await (const it of splitedChunks) {
    groups.push(it)
  }

  assert.deepEqual(
    groups,
    /* prettier-ignore */
    [
      [true , [[1]]],
      [false, [[1]]],
      [true , [[2]]],
      [true , [[3]]],
      [true , [[4], [4], [4], [4]]],
      [true , [[5], [5], [5]]],
      [false, [[5], [5]]],
      [true , [[6], [6], [6]]],
      [true , [[7], [7]]],
      [true , [[8], [8]]],
      [true , [[9]]],
      [true , [[10]]],
      [true , [[11]]],
      [true , [[12], [12]]],
      [true , [[13]]],
      [false, [[13], [13], [13]]],
      [false, [[13], [13], [13]]],
      [true , [[14], [14], [14]]],
      [false, [[14], [14], [14]]],
      [false, [[14]]],
      [true , [[15]]],
      [true , [[16]]]
    ]
  )
})
