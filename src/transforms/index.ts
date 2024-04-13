export * from './TransformState/index.js'
export * as column from './column/index.js'

export interface TransformExpressionParams {
  /** Column name */
  columnName: string

  /** Expression */
  expression: string
}
