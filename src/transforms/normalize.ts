import {
  getChunkNormalizer,
  isHeaderNormalized,
  TableChunksTransformer
} from '../index.js'
import { TransformBaseParams } from './index.js'

export interface NormalizeParams extends TransformBaseParams {
  immutable: boolean
}

/**
 * Normalize stream
 */
export const normalize = (params?: NormalizeParams): TableChunksTransformer => {
  const { immutable = false } = params ?? {}

  return source => {
    const tableHeader = source.getTableHeader()

    if (!immutable && isHeaderNormalized(tableHeader)) {
      return source
    }

    const { normalizedHeader, chunkNormalizer } = getChunkNormalizer(
      tableHeader,
      immutable
    )

    return {
      ...source,
      getTableHeader: () => normalizedHeader,
      async *[Symbol.asyncIterator]() {
        for await (const chunk of source) {
          yield chunkNormalizer(chunk)
        }
      }
    }
  }
}
