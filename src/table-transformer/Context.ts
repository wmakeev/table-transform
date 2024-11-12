import { LaneContext } from '../types/index.js'

export class Context implements LaneContext {
  #context

  constructor(
    initialContext?: Map<string | Symbol, unknown> | undefined | null
  ) {
    this.#context = initialContext ?? new Map<string | Symbol, unknown>()
  }

  getValue(key: string | Symbol): unknown {
    return this.#context.get(key)
  }

  setValue(key: string | Symbol, value: unknown): boolean {
    const hasValue = this.#context.has(key)
    this.#context.set(key, value)
    return hasValue
  }
}
