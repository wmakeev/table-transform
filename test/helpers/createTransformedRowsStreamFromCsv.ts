import { parse } from 'csv-parse'
import { createReadStream } from 'node:fs'
import path from 'node:path'
import {
  Readable,
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import {
  ChunkTransform,
  FlattenTransform,
  TableTransformer
} from '../../src/index.js'

export interface TransformedRowsStreamFromCsvOptions {
  highWaterMark?: number
  encoding?: 'utf8'
  bom?: boolean
  batchSize?: number
}

export const createTransformedRowsStreamFromCsv = (
  csvRelativePath: string,
  transformer: TableTransformer,
  options: TransformedRowsStreamFromCsvOptions = {}
): Readable => {
  const {
    highWaterMark = 16 * 1024,
    encoding = 'utf8',
    bom = true,
    batchSize = 100
  } = options

  return compose(
    createReadStream(path.join(process.cwd(), csvRelativePath), {
      highWaterMark,
      encoding
    }),

    parse({ bom }),

    new ChunkTransform({ batchSize }),

    transformer,

    new FlattenTransform()
  )
}
