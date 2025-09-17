import { ExpressionCompileProvider } from '../index.js'
import { TransformExpressionContext } from '../transforms/index.js'

export type ContextScopeMap<T = unknown> = Map<string | Symbol, T>

export const transformContextScopeSymbol = Symbol('transformContextScope')

export class Context {
  #parentContext: Context | null = null
  #currentContextScopeMap

  constructor(parent?: Context) {
    this.#currentContextScopeMap = new Map<Symbol, ContextScopeMap>()
    this.#parentContext = parent ?? null
  }

  getScopeMapWithKey(
    scope: Symbol,
    key: string | Symbol
  ): ContextScopeMap | undefined {
    const scopeMap = this.#currentContextScopeMap.get(scope)

    if (scopeMap?.has(key) === true) {
      return scopeMap
    }

    return this.#parentContext?.getScopeMapWithKey(scope, key)
  }

  has(scope: Symbol, key: string | Symbol): boolean {
    const scopeMap = this.getScopeMapWithKey(scope, key)
    return scopeMap != null
  }

  get<T = unknown>(scope: Symbol, key: string | Symbol): T | undefined {
    const scopeMap = this.getScopeMapWithKey(scope, key)

    if (scopeMap == null) return undefined

    return scopeMap.get(key) as T
  }

  set<T>(scope: Symbol, key: string | Symbol, value: T): boolean {
    let curScopeMap = this.#currentContextScopeMap.get(scope)

    if (curScopeMap === undefined) {
      curScopeMap = new Map()
      this.#currentContextScopeMap.set(scope, curScopeMap)
    }

    const hasValue = curScopeMap.has(key)

    curScopeMap.set(key, value)

    return !hasValue
  }

  // TODO Временное решение. Нужно подумать как реализовать через общий провайдер выражений.
  setExpressionContext(context: TransformExpressionContext) {
    this.set(transformContextScopeSymbol, 'context', context)
    return this
  }

  getExpressionContext() {
    return this.get(
      transformContextScopeSymbol,
      'context'
    ) as TransformExpressionContext
  }

  setExpressionCompileProvider(provider: ExpressionCompileProvider) {
    this.set(transformContextScopeSymbol, 'ExpressionCompileProvider', provider)
    return this
  }

  getExpressionCompileProvider(): ExpressionCompileProvider {
    return this.get(
      transformContextScopeSymbol,
      'ExpressionCompileProvider'
    ) as ExpressionCompileProvider
  }
}
