import {
  compileExpression,
  useDotAccessOperatorAndOptionalChaining
} from '@wmakeev/filtrex'
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
    customProp: useDotAccessOperatorAndOptionalChaining,

    symbols: {
      ...(context?.symbols ?? {}),

      value: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const actualColumnName = columnName ?? params.column

        if (actualColumnName == null) return null

        const index =
          transformState.fieldIndexesByName.get(actualColumnName)?.[
            transformState.arrColIndex
          ]

        return index === undefined ? '' : (transformState.curRow[index] ?? '')
      },

      values: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const actualColumnName = columnName ?? params.column

        if (actualColumnName == null) return []

        const indexes = transformState.fieldIndexesByName.get(actualColumnName)

        if (indexes === undefined) return ''

        return indexes.map(i => transformState.curRow[i] ?? '')
      },

      row: () => transformState.rowNum,

      column: () => transformState.column,

      // TODO Name is not obvious
      arrayIndex: () => transformState.arrColIndex,

      empty: (val: unknown) => {
        return val == null || val === ''
      }
    }
  })

  return transformExpression
}
