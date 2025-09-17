import { Context } from '../../index.js'
import {
  TransformExpressionParams,
  TransformExpressionState
} from '../index.js'

export const compileTransformExpression = (
  transformState: TransformExpressionState,
  params: TransformExpressionParams,
  context: Context
) => {
  const value = (columnName: unknown) => {
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
  }

  const values = (columnName: unknown) => {
    if (columnName != null && typeof columnName !== 'string') {
      throw new Error('values() argument expected to be string')
    }

    const actualColumnName = columnName ?? params.column

    if (actualColumnName == null) return []

    const indexes = transformState.fieldIndexesByName.get(actualColumnName)

    if (indexes === undefined) return ''

    return indexes.map(i => transformState.curRow[i] ?? '')
  }

  const expressionCompileProvider = context.getExpressionCompileProvider()

  if (expressionCompileProvider == null) {
    // TODO Use typed Error
    throw new Error(`ExpressionCompileProvider is not set`)
  }

  const expressionContext = context.getExpressionContext() ?? {}

  const transformExpression = expressionCompileProvider.compileExpression(
    params.expression,
    {
      ...expressionContext,
      value,
      values,
      row: () => transformState.rowNum,
      column: () => transformState.column,
      // TODO Name is not obvious
      arrayIndex: () => transformState.arrColIndex
    }
  )

  return transformExpression
}
