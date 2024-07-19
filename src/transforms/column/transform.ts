import assert from 'node:assert'
import { TableChunksTransformer } from '../../index.js'
import {
  ColumnTransformExpressionParams,
  TransformExpressionContext,
  TransformState
} from '../index.js'

export const transform = (
  params: ColumnTransformExpressionParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  assert.ok(
    typeof params.columnName === 'string',
    'transform columnName parameter expected to be string'
  )

  return async ({ header, getSourceGenerator }) => {
    const transformState: TransformState = new TransformState(
      params,
      header,
      context
    )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of getSourceGenerator()) {
        chunk.forEach(row => {
          transformState.nextRow(row)

          for (const [
            arrIndex,
            colIndex
          ] of transformState.fieldColsIndexes.entries()) {
            transformState.arrColIndex = arrIndex

            const result = transformState.evaluateExpression()

            row[colIndex] = result
          }
        })

        yield chunk
      }
    }

    return {
      header,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
