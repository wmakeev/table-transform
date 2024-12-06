import {
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

    const { normalizedHeader, chunkNormalizer } = getChunkNormalizer(
      header,
      immutable
    )

    return {
      ...source,
      getHeader: () => normalizedHeader,
      async *[Symbol.asyncIterator]() {
        for await (const chunk of source) {
          yield chunkNormalizer(chunk)
        }
      }
    }
  }
}
