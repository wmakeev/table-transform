import { TableChunksTransformer, TableRow } from '../index.js'

export interface TapRowsParams {
  tapFunction: (chunk: TableRow[]) => void
}

// const TRANSFORM_NAME = 'TapRows'

/**
 * TapRows
 */
export const tapRows = (params: TapRowsParams): TableChunksTransformer => {
  const { tapFunction } = params

  return source => {
    return {
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          tapFunction(chunk)
          yield chunk
        }
      }
    }
  }
}
