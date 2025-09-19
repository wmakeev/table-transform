import {
  TransformStepRowAssertError,
  TransformStepRowExpressionError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableRow } from '../../index.js'
import {
  TransformExpressionParams,
  TransformExpressionState
} from '../index.js'

const TRANSFORM_NAME = 'Column:Assert'

export const assert = (
  params: TransformExpressionParams & {
    // TODO Rename to "description"
    message?: string
  }
): TableChunksTransformer => {
  if (params.column == null && params.columnIndex != null) {
    throw new TransformStepError(
      'columnIndex cannot be specified without column',
      TRANSFORM_NAME
    )
  }

  return source => {
    async function* getTransformedSourceGenerator() {
      const internalTransformContext = source.getContext()

      const srcHeader = source.getTableHeader()

      const transformState = new TransformExpressionState(
        TRANSFORM_NAME,
        params,
        srcHeader,
        internalTransformContext
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
          if (transformState.curColSrcIndexes == null) {
            const result = transformState.evaluateExpression()

            if (result instanceof Error) {
              throw new TransformStepRowExpressionError(
                ...getErrArgs(result.message, chunk, rowIndex, null, result)
              )
            }

            if (typeof result !== 'boolean') {
              throw new TransformStepRowExpressionError(
                ...getErrArgs(
                  'Assert expression should return boolean result',
                  chunk,
                  rowIndex,
                  null
                )
              )
            }

            if (!result) {
              throw new TransformStepRowAssertError(
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
            ] of transformState.curColSrcIndexes.entries()) {
              if (
                params.columnIndex != null &&
                arrColIndex !== params.columnIndex
              ) {
                continue
              }

              transformState.arrColIndex = arrColIndex

              const result = transformState.evaluateExpression()

              if (result instanceof Error) {
                throw new TransformStepRowExpressionError(
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
                throw new TransformStepRowExpressionError(
                  ...getErrArgs(
                    'Assert expression should return boolean result',
                    chunk,
                    rowIndex,
                    headerColIndex
                  )
                )
              }

              if (!result) {
                throw new TransformStepRowAssertError(
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
      ...source,
      getTableHeader: () => source.getTableHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
