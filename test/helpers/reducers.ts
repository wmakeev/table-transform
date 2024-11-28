import { TableChunksReducer } from '../../src/index.js'
import { setImmediate } from 'node:timers/promises'

export const getFieldSumReducer: (column: string) => TableChunksReducer =
  column => src => {
    const sumColHeader = src
      .getHeader()
      .find(h => h.isDeleted === false && h.name === column)

    if (sumColHeader === undefined) {
      throw new Error(`Column "${column}" not found`)
    }

    return {
      outputColumns: ['reduced'],
      async getResult() {
        let reducedSum = 0

        for await (const chunk of src) {
          reducedSum = chunk.reduce((res, row) => {
            const num = row[sumColHeader.index]

            return typeof num === 'number' ? res + num : res
          }, reducedSum)

          await setImmediate()
        }

        return [reducedSum]
      }
    }
  }
