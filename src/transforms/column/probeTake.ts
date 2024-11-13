import {
  ColumnHeader,
  TableChunksTransformer,
  TransformColumnsNotFoundError
} from '../../index.js'
import { ProbesMap, probesMapPropSymbol } from './index.js'

interface ProbeTakeColumnParams {
  key: string
  column: string
  arrIndex?: number
}

const TRANSFORM_NAME = 'Column:ProbeTake'

/**
 * Take probe from first row column
 */
export const probeTake = (
  params: ProbeTakeColumnParams
): TableChunksTransformer => {
  const { key, column, arrIndex = 0 } = params

  return source => {
    const header = source.getHeader()

    const probeColumns: ColumnHeader[] = source
      .getHeader()
      .filter(h => !h.isDeleted && h.name === column)

    const columnHeader = probeColumns[arrIndex]

    if (columnHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    const ctx = source.getContext()

    let probesMap = ctx.getValue(probesMapPropSymbol) as ProbesMap | undefined

    if (probesMap === undefined) {
      probesMap = new Map()
      ctx.setValue(probesMapPropSymbol, probesMap)
    }

    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        let isProbeTaken = false

        for await (const chunk of source) {
          if (!isProbeTaken) {
            const probeValue = chunk[0]![columnHeader.index]

            probesMap.set(key, probeValue)

            isProbeTaken = true
          }

          yield chunk
        }
      }
    }
  }
}
