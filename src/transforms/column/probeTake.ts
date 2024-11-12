import {
  ColumnHeader,
  TableChunksTransformer,
  TransformColumnsNotFoundError
} from '../../index.js'
import { probesMapPropSymbol } from './index.js'

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
    return {
      ...source,

      async *[Symbol.asyncIterator]() {
        const ctx = source.getContext()

        let probesMap = ctx.getValue(probesMapPropSymbol) as Map<
          string,
          unknown
        >

        if (probesMap === undefined) {
          probesMap = new Map<string, unknown>()
          ctx.setValue(probesMapPropSymbol, probesMap)
        }

        let isProbed = false

        const header = source.getHeader()

        const probeColumns: ColumnHeader[] = source
          .getHeader()
          .filter(h => !h.isDeleted && h.name === column)

        for await (const chunk of source) {
          if (!isProbed) {
            const columnHeader = probeColumns[arrIndex]

            if (columnHeader === undefined) {
              throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [
                column
              ])
            }

            const probe = chunk[0]![columnHeader.index]

            probesMap.set(key, probe)

            isProbed = true
          }

          yield chunk
        }
      }
    }
  }
}
