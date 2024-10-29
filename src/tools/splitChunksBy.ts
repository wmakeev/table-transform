import { TableChunksAsyncIterable, TableRow } from '../index.js'

const getRowsComparator = (columnIndexes: number[]) => {
  return (a: TableRow | null, b: TableRow): boolean => {
    if (a == null) return false
    return columnIndexes.every(index => a[index] === b[index])
  }
}

export async function* splitChunksBy(
  source: TableChunksAsyncIterable,
  keyColumns: string[]
): AsyncGenerator<[isGroupFistChunk: boolean, chunk: TableRow[]]> {
  const keyColumnsSet = new Set(keyColumns)

  const keyColumnsHeaders = source
    .getHeader()
    .filter(h => !h.isDeleted && keyColumnsSet.has(h.name))

  const keyColumnsIndexes = keyColumnsHeaders.map(h => h.index)

  const isRowsInSameGroup = getRowsComparator(keyColumnsIndexes)

  /** First row of current rows group */
  let curGroupFirstRow: TableRow | null = null

  for await (const chunk of source) {
    /**
     * Index of the row starting from which chunk has not yet been divided
     * into groups
     */
    let chunkOffsetIndex = 0

    const isGroupContinuation = isRowsInSameGroup(curGroupFirstRow, chunk[0]!)

    while (true) {
      /**
       * Group border. The index where the current group ends and the next one
       * begins
       */
      let chunkSplitIndex = -1 // default value

      // Search for groups border in current chunk
      for (let i = chunkOffsetIndex; i < chunk.length; i++) {
        if (isRowsInSameGroup(curGroupFirstRow, chunk[i]!) === false) {
          chunkSplitIndex = i
          break
        }
      }

      // The entire chunk is included in the current group
      if (chunkSplitIndex === -1) {
        // [a]
        if (chunkOffsetIndex === 0) {
          yield [!isGroupContinuation, chunk]
        }

        // [..., a]
        else {
          yield [true, chunk.slice(chunkOffsetIndex)]
        }

        // proceed with next chunk
        break
      }

      // [..., b][a]
      else if (chunkSplitIndex === 0) {
        curGroupFirstRow = [...chunk[0]!]
        continue
      }

      // chunkSplitIndex > 0
      // [..., a, ...]
      else {
        yield [
          chunkOffsetIndex === 0 ? !isGroupContinuation : true,
          chunk.slice(chunkOffsetIndex, chunkSplitIndex)
        ]

        curGroupFirstRow = [...chunk[chunkSplitIndex]!]
        chunkOffsetIndex = chunkSplitIndex
      }
    }
  }
}
