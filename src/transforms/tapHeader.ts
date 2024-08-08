import { ColumnHeader, TableChunksTransformer } from '../index.js'

export interface TapHeaderParams {
  tapFunction: (header: ColumnHeader[]) => void
}

// const TRANSFORM_NAME = 'TapHeader'

/**
 * TapHeader
 */
export const tapHeader = (params: TapHeaderParams): TableChunksTransformer => {
  const { tapFunction } = params

  return source => {
    tapFunction(source.getHeader())
    return source
  }
}
