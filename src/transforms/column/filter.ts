import { TableChunksTransformer, TableRow } from '../../index.js'
import {
  TransformExpressionContext,
  TransformExpressionParams,
  TransformState
} from '../index.js'

export const filter = (
  params: TransformExpressionParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  return async ({ header, getSourceGenerator }) => {
    const transformState = new TransformState(params, header, context)

    async function* getTransformedSourceGenerator() {
      for await (const chunk of getSourceGenerator()) {
        const filteredRows: TableRow[] = []

        for (const row of chunk) {
          transformState.nextRow(row)

          let isPass = true

          // No column specified
          if (transformState.fieldColsIndexes.length === 0) {
            const result = transformState.evaluateExpression()

            if (result instanceof Error) throw result

            if (typeof result !== 'boolean') {
              throw new Error('Filter expression should return boolean result')
            }

            isPass = result
          }

          // Column specified
          else {
            for (const arrColIndex of transformState.fieldColsIndexes.keys()) {
              transformState.arrColIndex = arrColIndex

              const result = transformState.evaluateExpression()

              if (result instanceof Error) throw result

              if (typeof result !== 'boolean') {
                throw new Error(
                  'Filter expression should return boolean result'
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
      header,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
