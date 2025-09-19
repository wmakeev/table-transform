import { setTimeout as setTimeoutAsync } from 'node:timers/promises'
import {
  TableChunksTransformer,
  TransformStepColumnsNotFoundError,
  TransformStepRowError
} from '../index.js'

export interface WaitColumnParams {
  timeoutColumn: string
}

const TRANSFORM_NAME = 'Wait'

/**
 * Wait
 */
export const wait = (params: WaitColumnParams): TableChunksTransformer => {
  const { timeoutColumn } = params

  return source => {
    const tableHeader = source.getTableHeader()

    const colHeader = tableHeader.find(
      h => !h.isDeleted && h.name === timeoutColumn
    )

    if (colHeader === undefined) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        timeoutColumn
      ])
    }

    return {
      ...source,
      getTableHeader: () => tableHeader,
      [Symbol.asyncIterator]: async function* () {
        for await (const chunk of source) {
          for (const [rowIndex, row] of chunk.entries()) {
            const timeout = row[colHeader.index]

            if (typeof timeout !== 'number') {
              throw new TransformStepRowError(
                `Timeout expected to be a number, got ${typeof timeout}`,
                TRANSFORM_NAME,
                tableHeader,
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
