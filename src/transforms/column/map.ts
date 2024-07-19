import { ColumnHeader, TableChunksTransformer } from '../../index.js'

interface MapColumnParams {
  columnName: string
  mapper: (value: unknown) => unknown
  arrIndex?: number
}

/**
 * Map column values with specified function
 */
export const map = (params: MapColumnParams): TableChunksTransformer => {
  const { columnName, mapper, arrIndex } = params

  return async ({ header, getSourceGenerator }) => {
    const mappingColumns: ColumnHeader[] = header.filter(
      h => h.name === columnName
    )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of getSourceGenerator()) {
        chunk.forEach(row => {
          mappingColumns!.forEach((h, index) => {
            if (typeof arrIndex === 'number' && index !== arrIndex) return

            const value = row[h.index]

            row[h.index] = mapper(value)
          })
        })

        yield chunk
      }
    }

    return {
      header,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
