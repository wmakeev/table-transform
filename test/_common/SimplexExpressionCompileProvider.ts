import { compile } from 'simplex-lang'
import {
  ExpressionCompileProvider,
  ExpressionContext
} from '../../src/index.js'

export class SimplexExpressionCompileProvider
  implements ExpressionCompileProvider
{
  compileExpression(
    expression: string,
    context?: ExpressionContext
  ): (obj: any) => any {
    return compile(expression, {
      globals: context
    })
  }
}
