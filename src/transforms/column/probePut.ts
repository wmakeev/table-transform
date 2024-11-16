import {
  TableChunksTransformer,
  TableRow,
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../index.js'
import { ProbesMap, probesMapPropSymbol } from './index.js'

interface ProbePutColumnParams {
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
    const header = source.getHeader()

    const colHeader = header.find(h => !h.isDeleted && h.name === column)

    if (colHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    const ctx = source.getContext()

    let probesMap = ctx.getValue(probesMapPropSymbol) as ProbesMap

    if (probesMap === undefined) {
      probesMap = new Map()
      ctx.setValue(probesMapPropSymbol, probesMap)
    }

    const colIndex = colHeader.index

    const fillChunkColumn = (chunk: TableRow[], value: unknown) => {
      for (const row of chunk) {
        row[colIndex] = value
      }
    }

    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        let isProbeTaked = false
        let probedValue = undefined

        let chunksCache: TableRow[][] | undefined = []

        for await (const chunk of source) {
          if (isProbeTaked === false) {
            if (!probesMap.has(key)) {
              chunksCache?.push(chunk)
              continue
            }

            probedValue = probesMap.get(key)

            if (chunksCache !== undefined && chunksCache.length > 0) {
              for (const chunk of chunksCache) {
                fillChunkColumn(chunk, probedValue)
                yield chunk
              }
            }

            isProbeTaked = true

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
