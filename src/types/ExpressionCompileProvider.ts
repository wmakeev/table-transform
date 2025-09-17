import { ExpressionContext } from './ExpressionContext.js'

export interface ExpressionCompileProvider {
  compileExpression(
    expression: string,
    context: ExpressionContext
  ): (obj: any) => any
}
