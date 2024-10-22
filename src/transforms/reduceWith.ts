import {
  createTableHeader,
  TableChunksReducer,
  TableChunksTransformer
} from '../index.js'

export interface ReduceWithParams {
  reducer: TableChunksReducer
}

// const TRANSFORM_NAME = 'ReduceWith'

/**
 * Reduce data from source with custom reducer
 */
export const reduceWith = (
  params: ReduceWithParams
): TableChunksTransformer => {
  const { reducer } = params

  return source => {
    const { outputColumns, getResult } = reducer(source)

    const outHeader = createTableHeader(outputColumns)

    async function* getTransformedSourceGenerator() {
      const resultRow = await getResult()
      yield [resultRow]
    }

    return {
      getHeader: () => outHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
