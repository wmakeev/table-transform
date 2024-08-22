import {
  TransformRowAssertError,
  TransformRowExpressionError
} from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'
import {
  TransformExpressionContext,
  TransformExpressionParams,
  TransformState
} from '../index.js'

const TRANSFORM_NAME = 'Column:Assert'

export const assert = (
  params: TransformExpressionParams & { message?: string },
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

      const getErrArgs = (
        message: string,
        chunk: TableRow[],
        rowIndex: number,
        columnIndex: number | null,
        cause?: Error
      ) => {
        return [
          message,
          TRANSFORM_NAME,
          srcHeader,
          chunk,
          rowIndex,
          columnIndex,
          params.expression,
          { rowNum: transformState.rowNum, cause }
        ] as const
      }

      for await (const chunk of source) {
        for (const [rowIndex, row] of chunk.entries()) {
          transformState.nextRow(row)

          // No column specified
          if (transformState.fieldColsIndexes.length === 0) {
            const result = transformState.evaluateExpression()

            if (result instanceof Error) {
              throw new TransformRowExpressionError(
                ...getErrArgs(result.message, chunk, rowIndex, null, result)
              )
            }

            if (typeof result !== 'boolean') {
              throw new TransformRowExpressionError(
                ...getErrArgs(
                  'Assert expression should return boolean result',
                  chunk,
                  rowIndex,
                  null
                )
              )
            }

            if (!result) {
              throw new TransformRowAssertError(
                ...getErrArgs(
                  params.message ?? 'Assertion is not pass',
                  chunk,
                  rowIndex,
                  null
                )
              )
            }
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
                  ...getErrArgs(
                    result.message,
                    chunk,
                    rowIndex,
                    headerColIndex,
                    result
                  )
                )
              }

              if (typeof result !== 'boolean') {
                throw new TransformRowExpressionError(
                  ...getErrArgs(
                    'Assert expression should return boolean result',
                    chunk,
                    rowIndex,
                    headerColIndex
                  )
                )
              }

              if (!result) {
                throw new TransformRowAssertError(
                  ...getErrArgs(
                    params.message ?? 'Assertion is not pass',
                    chunk,
                    rowIndex,
                    headerColIndex
                  )
                )
              }
            }
          }
        }

        yield chunk
      }
    }

    return {
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
