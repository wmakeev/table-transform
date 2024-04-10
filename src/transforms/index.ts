export * as column from './column/index.js'

export interface ExpressionTransformParams {
  /** Column name */
  columnName: string

  /** Filtrex expression */
  expression: string

  /** Filtrex constants */
  constants?: {
    [T: string]: any
  }
}
