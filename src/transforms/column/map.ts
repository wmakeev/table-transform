import { TableChunksTransformer, TableHeader } from '../../index.js'
import { TransformBaseParams } from '../index.js'

interface MapColumnParams extends TransformBaseParams {
  column: string
  mapper: (value: unknown) => unknown
  arrIndex?: number
}

/**
 * Map column values with specified function
 */
export const map = (params: MapColumnParams): TableChunksTransformer => {
  const { column, mapper, arrIndex } = params

  return source => {
    async function* getTransformedSourceGenerator() {
      const mappingColumns: TableHeader = source
        .getTableHeader()
        .filter(h => !h.isDeleted && h.name === column)

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
      ...source,
      getTableHeader: () => source.getTableHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
