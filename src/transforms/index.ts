export * from './assertNotEmpty.js'
export * as column from './column/index.js'
export * from './flatMapWithProvider.js'
export * from './mergeFork.js'
export * from './normalize.js'
export * from './splitIn.js'
export * from './tapHeader.js'
export * from './tapRows.js'
export * from './TransformState/index.js'

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
