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
export * from './splitIn.js'
export * from './take.js'
export * from './takeWhile.js'
export * from './tapHeader.js'
export * from './tapRows.js'
export * from './TransformState/index.js'

export const channelScopeSymbol = Symbol('ChannelScope')

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
