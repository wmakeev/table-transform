import {
  // @ts-expect-error no typings for compose
  compose
} from 'node:stream'
import { TransformAssertError } from '../errors/index.js'
import {
  ColumnHeader,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../index.js'

export interface FlatMapWithParams {
  /** Column refs to datasource */
  sourceRefColumn: string

  sourceProvider: (sourceRef: unknown) => AsyncGenerator<TableRow[]>

  outputHeaders: string[]

  transformSelection: 'ALL' | 'PASSED' | 'FIRST_PASSED'

  transformConfigs: TableTransfromConfig[]
}

/**
 * FlatMap data from source uri
 */
export const flatMapWith = (
  params: FlatMapWithParams
): TableChunksTransformer => {
  const {
    sourceRefColumn,
    sourceProvider,
    outputHeaders,
    transformConfigs,
    transformSelection
  } = params

  return async ({ header, getSourceGenerator }) => {
    const sourceUriColumnHeader: ColumnHeader | undefined = header.find(
      h => !h.isDeleted && h.name === sourceRefColumn
    )

    if (sourceUriColumnHeader === undefined) {
      throw new Error(`Source uri column "${sourceRefColumn}" not found`)
    }

    const resultHeader: ColumnHeader[] = outputHeaders.map((name, index) => ({
      index,
      name,
      isDeleted: false
    }))

    async function* getTransformedSourceGenerator(): AsyncGenerator<
      TableRow[]
    > {
      for await (const srcChunk of getSourceGenerator()) {
        for (const row of srcChunk) {
          console.log(row)

          const sourceRef = row[sourceUriColumnHeader!.index]

          if (sourceRef == null) {
            throw new Error(
              `Source reference in "${sourceRefColumn}" column is not defined`
            )
          }

          for (const tfConfig of transformConfigs) {
            const transformer = createTableTransformer({
              ...tfConfig,
              transforms: [
                ...tfConfig.transforms,
                tf.column.select({ columns: outputHeaders })
              ],
              skipHeader: true
            })

            const sourceGen = sourceProvider(sourceRef)

            const transformedSourceStream = compose(sourceGen, transformer)

            try {
              yield* transformedSourceStream

              if (transformSelection === 'FIRST_PASSED') continue
            } catch (err) {
              if (!(err instanceof TransformAssertError)) {
                throw err
              }

              if (transformSelection === 'PASSED') {
                console.log(err.message)
                continue
              }

              throw err
            }
          }
        }
      }
    }

    return {
      header: resultHeader,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
