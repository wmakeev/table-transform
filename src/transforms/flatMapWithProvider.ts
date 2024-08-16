import {
  ColumnHeader,
  SourceProvider,
  TableChunksTransformer,
  TableTransfromConfig,
  createTableHeader,
  createTableTransformer
} from '../index.js'

export interface FlatMapWithProviderParams {
  sourceProvider: SourceProvider
  outputColumns: string[]
  transformConfig?: TableTransfromConfig
}

// const TRANSFORM_NAME = 'FlatMapWithProvider'

/**
 * FlatMap data from source with provider
 */
export const flatMapWithProvider = (
  params: FlatMapWithProviderParams
): TableChunksTransformer => {
  const { sourceProvider, transformConfig = {}, outputColumns } = params

  return source => {
    const srcHeader = source.getHeader()

    const resultHeader: ColumnHeader[] = createTableHeader(outputColumns)

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        for (const row of chunk) {
          const transformer = createTableTransformer({
            ...transformConfig,
            outputHeader: {
              ...transformConfig?.outputHeader,
              forceColumns: outputColumns,
              skip: true
            }
          })

          yield* transformer(sourceProvider(srcHeader, [...row]))
        }
      }
    }

    return {
      getHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
