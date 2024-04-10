import { DataRowChunkTransformer } from '../../index.js'
import { TransformState } from '../TransformState/index.js'
import { ExpressionTransformParams } from '../index.js'

export const transform = (
  params: ExpressionTransformParams
): DataRowChunkTransformer => {
  let transformState: TransformState | null = null

  return async ({ header, rows, rowLength }) => {
    if (transformState === null) {
      transformState = new TransformState(params, header)
    }

    for (const row of rows) {
      transformState.nextRow(row)

      for (const [
        arrIndex,
        colIndex
      ] of transformState.fieldColsIndexes.entries()) {
        transformState.arrColIndex = arrIndex

        const result = transformState.evaluateExpression()

        row[colIndex] = result
      }
    }

    return {
      header,
      rows,
      rowLength
    }
  }
}
