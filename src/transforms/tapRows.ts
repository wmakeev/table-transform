import { ColumnHeader, TableChunksTransformer, TableRow } from '../index.js'

export interface TapRowsParams {
  tapFunction: (chunk: TableRow[], header: ColumnHeader[]) => void
}

// const TRANSFORM_NAME = 'TapRows'

/**
 * TapRows
 */
export const tapRows = (params: TapRowsParams): TableChunksTransformer => {
  const { tapFunction } = params

  return source => {
    const header = source.getHeader()

    return {
      getHeader: () => header,
      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          tapFunction(chunk, header)
          yield chunk
        }
      }
    }
  }
}
