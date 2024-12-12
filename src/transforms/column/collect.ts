import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { createTableHeader, TableChunksTransformer } from '../../index.js'

const TRANSFORM_NAME = 'Column:Collect'

export interface CollectColumnParams {
  column: string
  resultColumn?: string
}

/**
 * Collect all column values
 */
export const collect = (
  params: CollectColumnParams
): TableChunksTransformer => {
  const { column, resultColumn = column } = params

  const newHeader = createTableHeader([resultColumn])

  return source => {
    const header = source.getHeader()

    const collectColumnHeader = header.find(
      h => !h.isDeleted && h.name === column
    )

    if (collectColumnHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    return {
      ...source,
      getHeader: () => newHeader,

      async *[Symbol.asyncIterator]() {
        const collectedValues: unknown[] = []

        for await (const chunk of source) {
          for (const row of chunk) {
            collectedValues.push(row[collectColumnHeader.index])
          }
        }

        if (collectedValues.length === 0) return

        yield [[collectedValues]]
      }
    }
  }
}
