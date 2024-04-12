import { compileExpression } from '@wmakeev/filtrex'
import { TransformState } from './index.js'
import { TransformExpressionParams } from '../index.js'

export interface TransformExpressionContext {
  functions?:
    | {
        [k: string]: (...args: any[]) => any
      }
    | undefined

  constants?:
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
    extraFunctions: {
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
      },

      ...(context?.functions ?? {})
    },

    constants: context?.constants ?? {}
  })

  return transformExpression
}
