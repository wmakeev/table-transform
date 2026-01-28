import { TableChunksTransformer, TableHeader, TableRow } from '../index.js'
import { TransformBaseParams } from './index.js'

export interface TapRowsParams extends TransformBaseParams {
  tapFunction: (chunk: TableRow[], header: TableHeader) => void
}

// const TRANSFORM_NAME = 'TapRows'

/**
 * TapRows
 */
export const tapRows = (params: TapRowsParams): TableChunksTransformer => {
  const { tapFunction } = params

  return source => {
    const tableHeader = source.getTableHeader()

    return {
      ...source,
      getTableHeader: () => tableHeader,
      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          tapFunction(chunk, tableHeader)
          yield chunk
        }
      }
    }
  }
}
