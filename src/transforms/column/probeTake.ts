import {
  TableChunksTransformer,
  TableHeader,
  TransformStepColumnsNotFoundError
} from '../../index.js'
import { probeScopeSymbol } from './index.js'

interface ProbeTakeColumnParams {
  column: string
  key?: string
  arrIndex?: number
}

const TRANSFORM_NAME = 'Column:ProbeTake'

/**
 * Take probe from first row column
 */
export const probeTake = (
  params: ProbeTakeColumnParams
): TableChunksTransformer => {
  const { column, key = column, arrIndex = 0 } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const probeColumns: TableHeader = source
      .getTableHeader()
      .filter(h => !h.isDeleted && h.name === column)

    const columnHeader = probeColumns[arrIndex]

    if (columnHeader === undefined) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    const ctx = source.getContext()

    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        let isProbeTaken = false

        for await (const chunk of source) {
          if (!isProbeTaken) {
            const probeValue = chunk[0]![columnHeader.index]

            ctx.set(probeScopeSymbol, key, probeValue)

            isProbeTaken = true
          }

          yield chunk
        }
      }
    }
  }
}
