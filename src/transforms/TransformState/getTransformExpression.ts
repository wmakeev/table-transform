import { compileExpression } from '@wmakeev/filtrex'
import { functions } from '../../functions/index.js'
import { CURRY_PLACEHOLDER } from '../../functions/curryWrapper.js'
import { TransformState } from './index.js'
import { ExpressionTransformParams } from '../index.js'

const defaultConstants = {
  _: CURRY_PLACEHOLDER,
  TRUE: true,
  FALSE: false
}

export const getTransformExpression = (
  params: ExpressionTransformParams,
  transformState: TransformState
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

      ...functions
    },

    constants: {
      ...defaultConstants,
      ...params.constants
    }
  })

  return transformExpression
}
