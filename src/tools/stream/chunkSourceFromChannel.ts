import assert from 'node:assert/strict'
import {
  ColumnHeader,
  HeaderChunkTuple,
  TableRow,
  transforms as tf
} from '../../index.js'
import { AsyncChannel } from '../index.js'

export interface ChunkSourceFromChannelParams {
  channel: AsyncChannel<HeaderChunkTuple>
  outputColumns?: string[]
}

/** Read chunks from channel */
export const chunkSourceFromChannel: (
  params: ChunkSourceFromChannelParams
) => AsyncIterable<TableRow[]> = ({
  channel,
  outputColumns
}): AsyncIterable<TableRow[]> => ({
  async *[Symbol.asyncIterator]() {
    /** @type {string[] | undefined} */
    let pickedColumnsNames: string[] | undefined = outputColumns ?? undefined

    /** @type {ColumnHeader[] | null} */
    let pickedHeader: ColumnHeader[] | null = null

    for await (const [header, chunk] of channel) {
      const actualHeader = header.filter(h => !h.isDeleted)

      // const stopFlagColumnIndex = actualHeader.find(
      //   h => h.name === stopFlagColumn
      // )?.index

      // if (stopFlagColumnIndex !== undefined) {
      //   throw new Error(
      //     `Channel close trigger column "${stopFlagColumn}" not found`
      //   )
      // }

      if (pickedHeader === null) {
        pickedHeader = header

        if (pickedColumnsNames == null) {
          pickedColumnsNames = actualHeader.map(h => h.name)
        }

        yield [[...pickedColumnsNames]] // header
      }

      assert.ok(pickedColumnsNames)

      const selectTransform = tf.column.select({
        columns: pickedColumnsNames,
        addMissingColumns: true
      })

      // TODO Extract logic from select to skip async iteration wrapping
      for await (const selectedChunk of selectTransform({
        getHeader: () => header,
        async *[Symbol.asyncIterator]() {
          yield chunk
        }
      })) {
        yield selectedChunk
      }
    }
  }
})
