import { CURRY_PLACEHOLDER } from './functions/curryWrapper.js'
import { functions } from './functions/index.js'
import { TransformExpressionContext } from './transforms/TransformState/getTransformExpression.js'

export const expressionDefaultConstants = {
  _: CURRY_PLACEHOLDER,
  TRUE: true,
  FALSE: false
}

export const expressionContext: TransformExpressionContext = {
  constants: expressionDefaultConstants,
  functions: functions
}
