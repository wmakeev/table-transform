import {
  ColumnHeader,
  TableRowFlatMapper,
  TableChunksTransformer,
  TableTransfromConfig,
  createRecordFromRow,
  createTableHeader,
  createTableTransformer,
  transforms as tf
} from '../index.js'

export interface FlatMapWithParams {
  mapper: TableRowFlatMapper

  // TODO Этот праметр можно убрать т.к. есть column.probeTake/Put

  /**
   * Columns that will be added to the mapper result with constant values from
   * the corresponding columns of the source row.
   */
  passThroughColumns?: string[]

  /**
   * The inner transformation that the mapper result goes through with the added
   * columns from the original row specified in `passThroughColumns`.
   */
  transformConfig?: TableTransfromConfig

  /**
   * Columns that should be selected (select transform applied) from inner
   * transform output.
   */
  outputColumns: string[]
}

// const TRANSFORM_NAME = 'FlatMapWith'

/**
 * FlatMap data from source with custom mapper
 */
export const flatMapWith = (
  params: FlatMapWithParams
): TableChunksTransformer => {
  const { mapper, transformConfig = {}, outputColumns } = params

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
                      column: col,
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

          yield* transformer(mapper(srcHeader, [...row]))
        }
      }
    }

    return {
      ...source,
      getHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
