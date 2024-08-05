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

  return async ({ header, getSourceGenerator }) => {
    return {
      header,
      getSourceGenerator: async function* () {
        for await (const chunk of getSourceGenerator()) {
          tapFunction(chunk)
          yield chunk
        }
      }
    }
  }
}
