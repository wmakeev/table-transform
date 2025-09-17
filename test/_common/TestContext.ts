import { Context, ExpressionContext } from '../../src/index.js'
import { FiltrexExpressionCompileProvider } from './FiltrexExpressionCompileProvider.js'

export const createTestContext = (context?: ExpressionContext) =>
  new Context()
    .setExpressionCompileProvider(new FiltrexExpressionCompileProvider())
    .setExpressionContext(context ?? {})
