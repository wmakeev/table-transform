import { setImmediate as setImmediateAsync } from 'node:timers/promises'
import { AsyncChannelError } from './errors.js'

export * from './errors.js'

export class AsyncChannel<T = unknown> {
  static readonly CLOSED: unique symbol = Symbol('CLOSED')

  #name: string | undefined = undefined

  #closed = false

  #buffer: T[] = []
  #bufferLength: number

  #putQueue = [] as Array<(closed?: boolean) => void>
  #takeQueue = [] as Array<(value: typeof AsyncChannel.CLOSED | T) => void>

  constructor(options?: { name?: string; bufferLength?: number }) {
    const { bufferLength = 0 } = options ?? {}

    this.#name = options?.name

    if (!Number.isInteger(bufferLength)) {
      throw new AsyncChannelError('buffer argument should be number')
    }

    if (bufferLength < 0) {
      throw new AsyncChannelError('buffer argument should be positive number')
    }

    this.#bufferLength = bufferLength
  }

  getName() {
    return this.#name
  }

  async put(value: T): Promise<boolean> {
    if (this.#closed) return false

    // Pass value directly for reader
    if (this.#buffer.length === 0 && this.#takeQueue.length > 0) {
      const resolve = this.#takeQueue.shift()!
      resolve(value)
      await setImmediateAsync()
      return true
    }

    // Buffer data
    if (this.#buffer.length < this.#bufferLength) {
      this.#buffer.push(value)
      return true
    }

    return new Promise(resolve => {
      this.#putQueue.push((closed = false) => {
        if (closed) {
          resolve(false)
        } else {
          this.#buffer.push(value)
          resolve(true)
        }
      })
    })
  }

  async take(): Promise<T | typeof AsyncChannel.CLOSED> {
    if (this.#closed) return AsyncChannel.CLOSED

    // Buffer is not full and writers waiting, let one writer to write to buffer
    if (
      this.#buffer.length <= this.#bufferLength &&
      this.#putQueue.length > 0
    ) {
      const nextWaitingWrite = this.#putQueue.shift()!
      nextWaitingWrite()
    }

    if (this.#buffer.length > 0) {
      return this.#buffer.shift()!
    }

    return await new Promise(resolve => this.#takeQueue.push(resolve))
  }

  close() {
    // TODO Нужно подумать как закрвать не деструктивно ..
    // .. напр. ждать опустошения очередей на чтение и запись
    this.#closed = true

    this.#takeQueue.forEach(reader => {
      reader(AsyncChannel.CLOSED)
    })

    this.#takeQueue = []

    this.#putQueue.forEach(writer => {
      writer(true)
    })

    this.#putQueue = []
  }

  isClosed() {
    return this.#closed
  }

  isFlushed() {
    return this.#buffer.length === 0 && this.#putQueue.length === 0
  }

  async flush(params?: {
    timeout?: number
    checkTimeout?: number
  }): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.isFlushed()) {
        resolve()
        return
      }

      // TODO Сделать на событиях без таймаутов

      let timeout: NodeJS.Timeout | undefined = undefined

      const interval = setInterval((): void => {
        if (this.isFlushed()) {
          clearTimeout(timeout)
          clearInterval(interval)
          resolve()
        }
      }, params?.checkTimeout ?? 10)

      if (params?.timeout != null) {
        timeout = setTimeout(() => {
          clearInterval(interval)

          if (this.isFlushed()) {
            resolve()
          } else {
            reject(new AsyncChannelError('AsyncChannel flush is timed out'))
          }
        }, params.timeout)
      }
    })
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Awaited<T>> {
    while (true) {
      const value = await this.take()

      if (value === AsyncChannel.CLOSED) {
        break
      }

      try {
        yield value
      } catch (err) {
        console.log('[AsyncChannel] generator error handled - ', err)
        throw err
      }
    }
  }
}
