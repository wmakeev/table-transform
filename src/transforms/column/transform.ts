import { DataRowChunkTransformer } from '../../index.js'
import { TransformExpressionContext } from '../TransformState/getTransformExpression.js'
import { TransformState } from '../TransformState/index.js'
import { TransformExpressionParams } from '../index.js'

export const transform = (
  params: TransformExpressionParams,
  context?: TransformExpressionContext
): DataRowChunkTransformer => {
  let transformState: TransformState | null = null

  return async ({ header, rows, rowLength }) => {
    if (transformState === null) {
      transformState = new TransformState(params, header, context)
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
