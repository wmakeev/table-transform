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

  return source => {
    async function* getTransformedSourceGenerator() {
      const srcHeader = source.getHeader()

      const transformState: TransformState = new TransformState(
        params,
        srcHeader,
        context
      )

      for await (const chunk of source) {
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
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
