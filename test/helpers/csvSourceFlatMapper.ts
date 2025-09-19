import { parse } from 'csv-parse'
import { createReadStream } from 'node:fs'
import path from 'node:path'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { ChunkTransform, createRecordFromRow } from '../../src/index.js'
import { TableRowFlatMapper } from '../../src/types/index.js'

export const csvSourceFlatMapper: TableRowFlatMapper = async function* (
  header,
  paramsRow
) {
  const options = createRecordFromRow(header, paramsRow)

  const filePath = options['file_path']

  if (typeof filePath !== 'string' || filePath === '') {
    throw new Error(`csvFlatMapper: filePath option not specified`)
  }

  const sourceStream: Readable = compose(
    createReadStream(path.join(process.cwd(), filePath), 'utf8'),
    parse({ bom: true }),
    new ChunkTransform({ batchSize: 2 })
  )

  yield* sourceStream
}
