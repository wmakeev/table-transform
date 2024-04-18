import { compileExpression } from '@wmakeev/filtrex'
import { TransformExpressionParams, TransformState } from '../index.js'

export interface TransformExpressionContext {
  symbols?:
    | {
        [T: string]: any
      }
    | undefined
}

export const getTransformExpression = (
  params: TransformExpressionParams,
  transformState: TransformState,
  context?: TransformExpressionContext
) => {
  const transformExpression = compileExpression(params.expression, {
    symbols: {
      ...(context?.symbols ?? {}),

      value: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const index = transformState.fieldIndexesByName.get(
          columnName ?? params.columnName
        )?.[transformState.arrColIndex]

        return index === undefined ? '' : transformState.curRow[index] ?? ''
      },

      values: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const index = transformState.fieldIndexesByName.get(
          columnName ?? params.columnName
        )

        if (index === undefined) return ''

        return index.map(i => transformState.curRow[i] ?? '')
      },

      row: () => transformState.rowNum,

      // TODO Name is not obvious
      arrayIndex: () => transformState.arrColIndex,

      empty: (val: unknown) => {
        return val == null || val === ''
      }
    }
  })

  return transformExpression
}
