import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'

const TRANSFORM_NAME = 'Column:Sort'

export interface SortColumnParams {
  column: string
  order?: 'asc' | 'desc'
}

const comparators = {
  asc: (index: number) => (a: TableRow, b: TableRow) => {
    const _a: any = a[index]
    const _b: any = b[index]

    if (_a > _b) return 1
    if (_a < _b) return -1
    return 0
  },

  desc: (index: number) => (a: TableRow, b: TableRow) => {
    const _a: any = a[index]
    const _b: any = b[index]

    if (_a < _b) return 1
    if (_a > _b) return -1
    return 0
  }
}

/**
 * Collect and sort by column value
 */
export const sort = (params: SortColumnParams): TableChunksTransformer => {
  const { column, order = 'asc' } = params

  return source => {
    const header = source.getHeader()

    const sortColumnHeader = header.find(h => !h.isDeleted && h.name === column)

    if (sortColumnHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [column])
    }

    return {
      ...source,
      getHeader: () => source.getHeader(),

      async *[Symbol.asyncIterator]() {
        const chunkCache: TableRow[][] = []

        for await (const chunk of source) {
          // TODO Optimize?
          chunkCache.push(chunk)
        }

        if (chunkCache[0] === undefined) return

        yield chunkCache.flat().sort(comparators[order](sortColumnHeader.index))
      }
    }
  }
}
