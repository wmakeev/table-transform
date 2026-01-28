export * from './assertNotEmpty.js'
export * as column from './column/index.js'
export * from './compose.js'
export * from './flatMapWith.js'
export * from './fork.js'
export * from './forkAndMerge.js'
export * from './forkToChannel.js'
export * as header from './header/index.js'
export * as internal from './internal/index.js'
export * from './mergeFromChannel.js'
export * from './normalize.js'
export * from './reduceWith.js'
export * from './rowsBuffer.js'
export * as sheet from './sheet/index.js'
export * from './skip.js'
export * from './splitIn.js'
export * from './take.js'
export * from './takeWhile.js'
export * from './tapHeader.js'
export * from './tapRows.js'
export * from './TransformExpressionState/index.js'
export * from './wait.js'

export const channelScopeSymbol = Symbol('ChannelScope')

export interface TransformBaseParams {
  /** Custom transform name */
  name?: string

  /** Transform description */
  description?: string
}

export interface TransformExpressionParams extends TransformBaseParams {
  /**
   * Optional column name.
   *
   * If column not specified, then `value()` expression returns `null`.
   */
  column?: string | undefined | null

  /** Expression */
  expression: string

  /** Index of array column */
  columnIndex?: number
}

export interface ColumnTransformExpressionParams extends TransformBaseParams {
  /** Column name */
  column: string

  /** Expression */
  expression: string

  /** Index of array column */
  columnIndex?: number
}
