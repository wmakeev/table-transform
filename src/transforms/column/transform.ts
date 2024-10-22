import assert from 'node:assert'
import { TableChunksTransformer } from '../../index.js'
import {
  ColumnTransformExpressionParams,
  TransformExpressionContext,
  TransformState
} from '../index.js'
import { TransformRowExpressionError } from '../../errors/index.js'

const TRANSFORM_NAME = 'Column:Transform'

export const transform = (
  params: ColumnTransformExpressionParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  assert.ok(
    typeof params.column === 'string',
    'transform column parameter expected to be string'
  )

  return source => {
    async function* getTransformedSourceGenerator() {
      const srcHeader = source.getHeader()

      const transformState: TransformState = new TransformState(
        TRANSFORM_NAME,
        params,
        srcHeader,
        context
      )

      for await (const chunk of source) {
        chunk.forEach((row, rowIndex) => {
          transformState.nextRow(row)

          for (const [
            arrColIndex,
            headerColIndex
          ] of transformState.fieldColsIndexes.entries()) {
            if (
              params.columnIndex != null &&
              arrColIndex !== params.columnIndex
            ) {
              continue
            }

            transformState.arrColIndex = arrColIndex

            const result = transformState.evaluateExpression()

            if (result instanceof Error) {
              throw new TransformRowExpressionError(
                result.message,
                TRANSFORM_NAME,
                srcHeader,
                chunk,
                rowIndex,
                headerColIndex,
                params.expression,
                { cause: result, rowNum: transformState.rowNum }
              )
            }

            row[headerColIndex] = result
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
