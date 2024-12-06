import assert from 'node:assert/strict'
import {
  ColumnHeader,
  createTableTransformer,
  HeaderChunkTuple,
  TableChunksSource,
  TableRow,
  transforms as tf
} from '../../index.js'
import { AsyncChannel } from '../index.js'
import { Context } from '../../table-transformer/Context.js'

export interface ChunkSourceFromChannelParams {
  channel: AsyncChannel<HeaderChunkTuple>
  outputColumns?: string[]

  // TODO Нужно думать над тем откуда брать контекст для канала при его создании
  // Можно/нужно ли передавать контекст вместе с чанками и заголовками?
  context: Context
}

/**
 * Read chunks from channel
 * @deprecated
 */
export const chunkSourceFromChannel: (
  params: ChunkSourceFromChannelParams
) => AsyncIterable<TableRow[]> = ({
  channel,
  outputColumns,
  context
}): AsyncIterable<TableRow[]> => ({
  async *[Symbol.asyncIterator]() {
    /** @type {string[] | undefined} */
    let pickedColumnsNames: string[] | undefined = outputColumns ?? undefined

    /** @type {ColumnHeader[] | null} */
    let pickedHeader: ColumnHeader[] | null = null

    for await (const [header, chunk] of channel) {
      const actualHeader = header.filter(h => !h.isDeleted)

      if (pickedHeader === null) {
        pickedHeader = header

        if (pickedColumnsNames == null) {
          pickedColumnsNames = actualHeader.map(h => h.name)
        }

        yield [[...pickedColumnsNames]] // header
      }

      assert.ok(pickedColumnsNames)

      const chunkIterable: TableChunksSource = {
        getContext() {
          return context
        },
        getHeader() {
          return header
        },
        async *[Symbol.asyncIterator]() {
          yield chunk
        }
      }

      const outGen = createTableTransformer({
        outputHeader: {
          skip: true
        },
        transforms: [
          tf.column.select({
            columns: pickedColumnsNames,
            addMissingColumns: true
          })
        ]
      })(chunkIterable)

      for await (const outChunk of outGen) {
        yield outChunk
      }
    }
  }
})
