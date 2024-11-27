import { setTimeout as setTimeoutAsync } from 'node:timers/promises'
import {
  TableChunksTransformer,
  TransformColumnsNotFoundError,
  TransformRowError
} from '../../index.js'

export interface WaitColumnParams {
  timeoutColumn: string
}

const TRANSFORM_NAME = 'Column:Wait'

/**
 * Wait
 */
export const wait = (params: WaitColumnParams): TableChunksTransformer => {
  const { timeoutColumn } = params

  return source => {
    const header = source.getHeader()

    const colHeader = header.find(h => !h.isDeleted && h.name === timeoutColumn)

    if (colHeader === undefined) {
      throw new TransformColumnsNotFoundError(TRANSFORM_NAME, header, [
        timeoutColumn
      ])
    }

    return {
      ...source,
      getHeader: () => header,
      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          for (const [rowIndex, row] of chunk.entries()) {
            const timeout = row[colHeader.index]

            if (typeof timeout !== 'number') {
              throw new TransformRowError(
                `Timeout expected to be a number, got ${typeof timeout}`,
                TRANSFORM_NAME,
                header,
                chunk,
                rowIndex,
                colHeader.index
              )
            }

            await setTimeoutAsync(timeout)

            yield [row]
          }
        }
      }
    }
  }
}
