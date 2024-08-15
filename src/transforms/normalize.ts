import {
  createTableHeader,
  getChunkNormalizer,
  isHeaderNormalized,
  TableChunksTransformer
} from '../index.js'

export interface NormalizeParams {
  immutable: boolean
}

/**
 * Normalize stream
 */
export const normalize = (params?: NormalizeParams): TableChunksTransformer => {
  const { immutable = false } = params ?? {}

  return source => {
    const header = source.getHeader()

    if (!immutable && isHeaderNormalized(header)) {
      return source
    }

    const actualHeader = header.filter(h => !h.isDeleted)
    const actualColumns = actualHeader.map(h => h.name)

    const normalizedHeader = createTableHeader(actualColumns)

    const chunkNormalizer = getChunkNormalizer(header, immutable)

    return {
      getHeader: () => normalizedHeader,
      async *[Symbol.asyncIterator]() {
        for await (const chunk of source) {
          yield chunkNormalizer(chunk)
        }
      }
    }
  }
}
