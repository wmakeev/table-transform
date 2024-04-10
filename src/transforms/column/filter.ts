import { DataRowChunk, DataRowChunkTransformer } from '../../index.js'
import { TransformState } from '../TransformState/index.js'
import { ExpressionTransformParams } from '../index.js'

export const filter = (
  params: ExpressionTransformParams
): DataRowChunkTransformer => {
  let transformState: TransformState | null = null

  return async ({ header, rows, rowLength }) => {
    const filteredRows: DataRowChunk = []

    if (transformState === null) {
      transformState = new TransformState(params, header)
    }

    for (const row of rows) {
      transformState.nextRow(row)

      let isPass = true

      for (const arrColIndex of transformState.fieldColsIndexes.keys()) {
        transformState.arrColIndex = arrColIndex

        const result = transformState.evaluateExpression()

        if (result instanceof Error) {
          throw result
        }

        if (typeof result !== 'boolean') {
          throw new Error('Filter expression should return boolean result')
        }

        isPass = result

        if (isPass === false) break
      }

      if (isPass) filteredRows.push(row)
    }

    return {
      header,
      rows: filteredRows,
      rowLength
    }
  }
}
