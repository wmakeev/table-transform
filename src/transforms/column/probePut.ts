import {
  TableChunksTransformer,
  TableRow,
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../index.js'
import { probeScopeSymbol } from './index.js'

export interface ProbePutColumnParams {
  column: string
  key?: string
  arrIndex?: number
}

const TRANSFORM_NAME = 'Column:ProbePut'

/**
 * Fill column with early probed value
 */
export const probePut = (
  params: ProbePutColumnParams
): TableChunksTransformer => {
  const { column, key = column } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const colHeader = tableHeader.find(h => !h.isDeleted && h.name === column)

    if (colHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    const ctx = source.getContext()

    const colIndex = colHeader.index

    const fillChunkColumn = (chunk: TableRow[], value: unknown) => {
      for (const row of chunk) {
        row[colIndex] = value
      }
    }

    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        let isProbeTaken = false
        let probedValue = undefined

        let chunksCache: TableRow[][] | undefined = []

        for await (const chunk of source) {
          if (isProbeTaken === false) {
            if (ctx.has(probeScopeSymbol, key) === false) {
              chunksCache?.push(chunk)
              continue
            }

            probedValue = ctx.get(probeScopeSymbol, key)

            if (chunksCache !== undefined && chunksCache.length > 0) {
              for (const chunk of chunksCache) {
                fillChunkColumn(chunk, probedValue)
                yield chunk
              }
            }

            isProbeTaken = true

            chunksCache = undefined
          }

          fillChunkColumn(chunk, probedValue)

          yield chunk
        }

        if (chunksCache !== undefined) {
          throw new TransformStepError(
            'Probe not taken in Column:ProbeTake',
            TRANSFORM_NAME
          )
        }
      }
    }
  }
}
