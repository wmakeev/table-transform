import { TransformRowExpressionError } from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'
import {
  TransformExpressionContext,
  TransformExpressionParams,
  TransformState
} from '../index.js'

const TRANSFORM_NAME = 'Column:Filter'

export const filter = (
  params: TransformExpressionParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  return source => {
    async function* getTransformedSourceGenerator() {
      const srcHeader = source.getHeader()

      const transformState = new TransformState(
        TRANSFORM_NAME,
        params,
        srcHeader,
        context
      )

      for await (const chunk of source) {
        const filteredRows: TableRow[] = []

        for (const [rowIndex, row] of chunk.entries()) {
          transformState.nextRow(row)

          let isPass = true

          // No column specified
          if (transformState.fieldColsIndexes.length === 0) {
            const result = transformState.evaluateExpression()

            if (result instanceof Error) {
              throw new TransformRowExpressionError(
                result.message,
                TRANSFORM_NAME,
                srcHeader,
                chunk,
                rowIndex,
                null,
                params.expression,
                { cause: result, rowNum: transformState.rowNum }
              )
            }

            if (typeof result !== 'boolean') {
              throw new TransformRowExpressionError(
                'Filter expression should return boolean result',
                TRANSFORM_NAME,
                srcHeader,
                chunk,
                rowIndex,
                null,
                params.expression,
                { rowNum: transformState.rowNum }
              )
            }

            isPass = result
          }

          // Column specified
          else {
            for (const [
              arrColIndex,
              headerColIndex
            ] of transformState.fieldColsIndexes.entries()) {
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

              if (typeof result !== 'boolean') {
                throw new TransformRowExpressionError(
                  'Filter expression should return boolean result',
                  TRANSFORM_NAME,
                  srcHeader,
                  chunk,
                  rowIndex,
                  headerColIndex,
                  params.expression,
                  { rowNum: transformState.rowNum }
                )
              }

              isPass = result

              if (isPass === false) break
            }
          }

          if (isPass) filteredRows.push(row)
        }

        yield filteredRows
      }
    }

    return {
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
