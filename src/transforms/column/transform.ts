import assert from 'node:assert'
import { TransformRowExpressionError } from '../../errors/index.js'
import { TableChunksTransformer } from '../../index.js'
import {
  ColumnTransformExpressionParams,
  TransformExpressionState
} from '../index.js'

const TRANSFORM_NAME = 'Column:Transform'

export const transform = (
  params: ColumnTransformExpressionParams
): TableChunksTransformer => {
  assert.ok(
    typeof params.column === 'string',
    'transform column parameter expected to be string'
  )

  return source => {
    async function* getTransformedSourceGenerator() {
      const internalTransformContext = source.getContext()

      const srcHeader = source.getHeader()

      const transformState: TransformExpressionState =
        new TransformExpressionState(
          TRANSFORM_NAME,
          params,
          srcHeader,
          internalTransformContext
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
      ...source,
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
