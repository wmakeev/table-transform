import {
  transformContextScope,
  TransformExpressionContext
} from '../transforms/index.js'

export type ContextScopeMap = Map<string | Symbol, unknown>

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

  get(scope: Symbol, key: string | Symbol): unknown {
    const scopeMap = this.getScopeMapWithKey(scope, key)

    if (scopeMap == null) return undefined

    return scopeMap.get(key)
  }

  set(scope: Symbol, key: string | Symbol, value: unknown): boolean {
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
  setTransformExpressionContext(context: TransformExpressionContext) {
    this.set(transformContextScope, 'context', context)
    return this
  }

  getTransformExpressionContext() {
    return this.get(
      transformContextScope,
      'context'
    ) as TransformExpressionContext
  }
}
