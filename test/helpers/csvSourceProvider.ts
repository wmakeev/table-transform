import { parse } from 'csv-parse'
import { createReadStream } from 'node:fs'
import path from 'node:path'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { ChunkTransform, getRowRecord } from '../../src/index.js'
import { SourceProvider } from '../../src/types.js'

export const csvSourceProvider: SourceProvider = async function* (
  header,
  paramsRow
) {
  const options = getRowRecord(header, paramsRow)

  const filePath = options['file_path']

  if (typeof filePath !== 'string' || filePath === '') {
    throw new Error(`csvSourceProvider: filePath option not specified`)
  }

  const sourceStream: Readable = compose(
    createReadStream(path.join(process.cwd(), filePath), 'utf8'),
    parse({ bom: true }),
    new ChunkTransform({ batchSize: 2 })
  )

  yield* sourceStream
}
