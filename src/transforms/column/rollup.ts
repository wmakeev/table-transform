import { TransformStepColumnsNotFoundError } from '../../errors/index.js'
import {
  createTableHeader,
  TableChunksTransformer,
  TableHeader
} from '../../index.js'
import { TransformBaseParams } from '../index.js'

const TRANSFORM_NAME = 'Column:Rollup'

export interface RollupColumnParams extends TransformBaseParams {
  columns: string[]
}

/**
 * Rollup all columns values
 */
export const rollup = (params: RollupColumnParams): TableChunksTransformer => {
  const { columns } = params

  const newHeader = createTableHeader(columns)

  return source => {
    const tableHeader = source.getTableHeader()

    const notFoundColumns = []
    const rollingUpColumnsHeaders: TableHeader = []

    for (const column of columns) {
      const colHeader = tableHeader.find(h => !h.isDeleted && h.name === column)
      if (colHeader === undefined) notFoundColumns.push(column)
      else rollingUpColumnsHeaders.push(colHeader)
    }

    if (notFoundColumns.length) {
      throw new TransformStepColumnsNotFoundError(
        TRANSFORM_NAME,
        tableHeader,
        notFoundColumns
      )
    }

    return {
      ...source,
      getTableHeader: () => newHeader,

      async *[Symbol.asyncIterator]() {
        const rolledValues = Array.from(
          { length: rollingUpColumnsHeaders.length },
          () => [] as unknown[]
        )

        for await (const chunk of source) {
          for (const row of chunk) {
            for (let i = 0; i < rollingUpColumnsHeaders.length; i++) {
              rolledValues[i]!.push(row[rollingUpColumnsHeaders[i]!.index])
            }
          }
        }

        if (rolledValues[0]!.length === 0) return

        yield [rolledValues]
      }
    }
  }
}
