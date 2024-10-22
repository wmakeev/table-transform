export * from './assertNotEmpty.js'
export * as column from './column/index.js'
export * from './flatMapWith.js'
export * from './forkToChannel.js'
export * from './mergeFork.js'
export * from './normalize.js'
export * from './reduceWith.js'
export * from './splitIn.js'
export * from './takeWhile.js'
export * from './tapHeader.js'
export * from './tapRows.js'
export * from './TransformState/index.js'

export interface TransformExpressionParams {
  /**
   * Optional column name.
   *
   * If column not specified, then `value()` experssion returns `null`.
   */
  column?: string | undefined | null

  /** Expression */
  expression: string

  /** Index of array column */
  columnIndex?: number
}

export interface ColumnTransformExpressionParams {
  /** Column name */
  column: string

  /** Expression */
  expression: string

  /** Index of array column */
  columnIndex?: number
}
