import assert from 'node:assert'
import { DataRowChunkTransformer } from '../../index.js'
import {
  TransformExpressionContext,
  ColumnTransformExpressionParams,
  TransformState
} from '../index.js'

export const transform = (
  params: ColumnTransformExpressionParams,
  context?: TransformExpressionContext
): DataRowChunkTransformer => {
  assert.ok(
    typeof params.columnName === 'string',
    'transform columnName parameter expected to be string'
  )

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
