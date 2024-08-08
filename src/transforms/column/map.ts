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

  return source => {
    async function* getTransformedSourceGenerator() {
      const mappingColumns: ColumnHeader[] = source
        .getHeader()
        .filter(h => h.name === columnName)

      for await (const chunk of source) {
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
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
