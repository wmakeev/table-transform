import {
  TransformRowExpressionError,
  TransformStepError
} from '../errors/index.js'
import { TableChunksTransformer, TableRow } from '../index.js'
import {
  TransformExpressionContext,
  TransformExpressionParams,
  TransformState
} from './index.js'

const TRANSFORM_NAME = 'TakeWhile'

// TODO Копипаста assert и прочих transform-like модулей. Можно ли обобщить?

export const takeWhile = (
  params: TransformExpressionParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  return source => {
    if (params.column == null && params.columnIndex != null) {
      throw new TransformStepError(
        'columnIndex cannot be specified without column',
        TRANSFORM_NAME
      )
    }

    async function* getTransformedSourceGenerator() {
      const internalTransformContext = source
        .getContext()
        .getTransformExpressionContext()

      const srcHeader = source.getHeader()

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

      // FIXME Не обработанная ошибка если некорректное выражение "Error: Parse error on line 1:"
      const transformState = new TransformState(
        TRANSFORM_NAME,
        params,
        srcHeader,
        internalTransformContext ?? context
      )

      let isBreaked = false

      for await (const chunk of source) {
        const passedRows: TableRow[] = []

        for (const [rowIndex, row] of chunk.entries()) {
          transformState.nextRow(row)

          let isPass = true

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
                  'Filter expression should return boolean result',
                  chunk,
                  rowIndex,
                  null
                )
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
                    'Filter expression should return boolean result',
                    chunk,
                    rowIndex,
                    headerColIndex
                  )
                )
              }

              isPass = result

              if (isPass === false) break
            }
          }

          if (isPass) {
            passedRows.push(row)
          } else {
            isBreaked = true
            break
          }
        }

        if (passedRows.length > 0) {
          yield passedRows
        }

        if (isBreaked) {
          break
        }
      }
    }

    return {
      ...source,
      getHeader: () => source.getHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
