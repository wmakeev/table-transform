import {
  ColumnHeader,
  SourceProvider,
  TableChunksTransformer,
  TableTransfromConfig,
  createRecordFromRow,
  createTableHeader,
  createTableTransformer,
  transforms as tf
} from '../index.js'

export interface FlatMapWithProviderParams {
  sourceProvider: SourceProvider

  /**
   * Source columns that should be passed to the output columns.
   */
  passThroughColumns?: string[]

  /**
   * Columns that should be selected from inner transform.
   */
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

  const passThroughColumns = [...new Set(params.passThroughColumns)]

  return source => {
    const srcHeader = source.getHeader()

    const resultHeader: ColumnHeader[] = createTableHeader(outputColumns)

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        for (const row of chunk) {
          const rowRecord =
            passThroughColumns.length > 0
              ? createRecordFromRow(srcHeader, row)
              : undefined

          const transformer = createTableTransformer({
            ...transformConfig,

            transforms: [
              ...(rowRecord
                ? passThroughColumns.map(col => {
                    return tf.column.add({
                      columnName: col,
                      defaultValue: rowRecord[col]
                    })
                  })
                : []),

              ...(transformConfig.transforms ?? [])
            ],

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
