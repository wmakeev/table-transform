export * from './TransformState/index.js'
export * as column from './column/index.js'
export * from './flatMapWithProvider.js'
export * from './mergeFork.js'
export * from './splitIn.js'
export * from './tapHeader.js'
export * from './tapRows.js'

export interface TransformExpressionParams {
  /**
   * Optional column name.
   *
   * If column not specified, then `value()` experssion returns `null`.
   */
  columnName?: string | undefined | null

  /** Expression */
  expression: string
}

export interface ColumnTransformExpressionParams {
  /** Column name */
  columnName: string

  /** Expression */
  expression: string
}
