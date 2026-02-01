import { Context } from '../../index.js'
import {
  TransformExpressionParams,
  TransformExpressionState
} from '../index.js'

export const compileTransformExpression = (
  state: TransformExpressionState,
  params: TransformExpressionParams,
  context: Context,
  errors: {
    createIncorrectColumnNameTypeError: (
      message: string,
      columnName: unknown
    ) => Error
    createColumnNotFoundError: (column: string, index?: number) => Error
    createColumnIndexOverBoundsError: (columnIndex: number) => Error
    createDefaultColumNotSetError: () => Error
  }
) => {
  const value = (column?: unknown) => {
    // Get current value - value()
    if (column == null) {
      if (state.curColDefaultSrcIndex === null) {
        throw errors.createDefaultColumNotSetError()
      }

      return state.arrColIndex === 0
        ? state.curRow[state.curColDefaultSrcIndex]
        : state.curRow[state.curColSrcIndexes![state.arrColIndex]!]
    }

    // Get value by column index - value(2)
    if (typeof column === 'number') {
      const colHeader = state.actualTableHeader[column]
      if (colHeader === undefined) {
        throw errors.createColumnIndexOverBoundsError(column)
      }
      return state.curRow[colHeader.index]
    }

    if (typeof column !== 'string') {
      throw errors.createIncorrectColumnNameTypeError(
        'value() argument expected to be string or number',
        column
      )
    }

    // Get value by column name - value('foo')

    const index = state.srcIndexesByColName.get(column)?.[0]

    if (index === undefined) {
      throw errors.createColumnNotFoundError(column, state.arrColIndex)
    }

    return state.curRow[index]
  }

  const values = (columnName?: unknown) => {
    // Get current values - values()
    if (columnName == null) {
      if (state.curColSrcIndexes === null) {
        throw errors.createDefaultColumNotSetError()
      }

      return state.curColSrcIndexes.map(i => state.curRow[i])
    }

    if (typeof columnName !== 'string') {
      throw errors.createIncorrectColumnNameTypeError(
        'values() argument expected to be string',
        columnName
      )
    }

    const indexes = state.srcIndexesByColName.get(columnName)

    if (indexes === undefined) {
      throw errors.createColumnNotFoundError(columnName)
    }

    // Get current values - values('foo')
    return indexes.map(i => state.curRow[i])
  }

  const expressionCompileProvider = context.getExpressionCompileProvider()

  if (expressionCompileProvider == null) {
    // TODO Use typed Error
    throw new Error(`ExpressionCompileProvider is not set`)
  }

  const expressionContext = context.getExpressionContext() ?? {}

  // TODO Компиляция функции происходит слишком поздно уже на этапе обработки ..
  // .. данных, хотя ошибку компиляции можно было бы выдать значительно раньше.
  // #dl9adgn1
  const transformExpression = expressionCompileProvider.compileExpression(
    params.expression,
    {
      ...expressionContext,
      value,
      values,
      row: () => state.rowNum,
      column: () => state.curColName,
      columns: () => state.rowColumns,
      columnIndex: (name?: string, fromIndex?: number) =>
        state.rowColumns.indexOf(name ?? state.curColName ?? '', fromIndex),
      arrColIndex: () => state.arrColIndex
    }
  )

  return transformExpression
}
