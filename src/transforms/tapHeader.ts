import { TableChunksTransformer, TableHeader } from '../index.js'

export interface TapHeaderParams {
  tapFunction: (header: TableHeader) => void
}

// const TRANSFORM_NAME = 'TapHeader'

/**
 * TapHeader
 */
export const tapHeader = (params: TapHeaderParams): TableChunksTransformer => {
  const { tapFunction } = params

  return source => {
    tapFunction(source.getTableHeader())
    return source
  }
}
