import {
  ExpressionCompileProvider,
  ExpressionContext,
  TransformStepColumnsNotFoundError,
  TransformError,
  TransformStepExprSymbolNotFoundError
} from '../../src/index.js'
import { compileExpression } from '@wmakeev/filtrex'

function optionalPropertyAccessor(
  name: string,
  get: (name: string) => any,
  obj: object | undefined,
  type: 'single-quoted' | 'unescaped',
  isNested: boolean
) {
  if (obj === null || obj === undefined) return obj

  // Force quoted data field access
  if (isNested === false && type !== 'single-quoted') {
    try {
      get(name)
    } catch (err) {
      if (err instanceof TransformStepColumnsNotFoundError) {
        throw new TransformStepExprSymbolNotFoundError(
          err.stepName,
          err.header,
          name
        )
      }
    }

    throw new TransformError(
      `Field "${name}" access with unquoted name - try to use quoted notation "'${name}'"`
    )
  }

  if (isNested) {
    try {
      return get(name)
    } catch (err) {
      if (err instanceof Error && err.name === 'ReferenceError') {
        return undefined
      }
      throw err
    }
  } else {
    return get(name)
  }
}

export class FiltrexExpressionCompileProvider
  implements ExpressionCompileProvider
{
  compileExpression(
    expression: string,
    context?: ExpressionContext
  ): (obj: any) => any {
    return compileExpression(expression, {
      customProp: optionalPropertyAccessor,
      symbols: context ?? {}
    })
  }
}
