import assert from 'node:assert'
import { Transform, TransformCallback, TransformOptions } from 'stream'

export interface DynamicHeaderTransformOptions {
  columnNameFrom: string
  columnValueFrom: string
}

export class DynamicHeaderTransform extends Transform {
  /** Incoming rows buffer */
  #rowsBuffer = [] as unknown[][]

  /** Headers */
  #header!: string[]
  #headersSet!: Set<string>
  #rowLen!: number

  #columnNameFromIndex!: number
  #columnValueFromIndex!: number

  #dynamicHeadersSet = new Set<string>()

  constructor(
    public options: TransformOptions & DynamicHeaderTransformOptions
  ) {
    super({
      ...options,
      readableObjectMode: true,
      writableObjectMode: true
    })
  }

  override _transform(
    chunk: unknown[][],
    _encoding: BufferEncoding,
    done: TransformCallback
  ): void {
    assert.ok(Array.isArray(chunk))
    assert.ok(Array.isArray(chunk[0]))

    // Init header
    if (this.#header === undefined) {
      this.#header = chunk.shift()!.map(h => String(h))
      this.#rowLen = this.#header.length
      this.#headersSet = new Set(this.#header)

      this.#columnNameFromIndex = this.#header.indexOf(
        this.options.columnNameFrom
      )

      if (this.#columnNameFromIndex === -1) {
        // TODO Нужно ли передавать ошибку в done?
        throw new Error(`Column "${this.options.columnNameFrom}" not found`)
      }

      this.#columnValueFromIndex = this.#header.indexOf(
        this.options.columnValueFrom
      )

      if (this.#columnValueFromIndex === -1) {
        throw new Error(`Column "${this.options.columnValueFrom}" not found`)
      }
    }

    for (const row of chunk) {
      assert.equal(row.length, this.#rowLen)

      const dynamicColumnName = row[this.#columnNameFromIndex]

      if (
        dynamicColumnName == null ||
        typeof dynamicColumnName !== 'string' ||
        dynamicColumnName === ''
      ) {
        throw new Error(`Dynamic column name should be not empty string`)
      }

      if (this.#headersSet.has(dynamicColumnName)) {
        throw new Error(
          `Dynamic column name "${dynamicColumnName}"` +
            ' overlap exist header column with same name'
        )
      }

      this.#dynamicHeadersSet.add(dynamicColumnName)
    }

    this.#rowsBuffer.push(...chunk)

    done()
  }

  override _final(done: TransformCallback): void {
    if (this.#rowsBuffer.length === 0) {
      done()
      return
    }

    const dynamicHeaders = [...this.#dynamicHeadersSet.values()]
    const dynamicHeadersLen = dynamicHeaders.length

    const dynamicHeadersIndexByName = new Map(
      [...dynamicHeaders.entries()].map(([index, h]) => [
        h,
        index + this.#header.length
      ])
    )

    const resultRows = [[...this.#header, ...dynamicHeaders]] as unknown[][]

    for (const row of this.#rowsBuffer) {
      row.push(...Array(dynamicHeadersLen))

      const dynamicColumnName = row[this.#columnNameFromIndex] as string
      const dynamicColumnIndex =
        dynamicHeadersIndexByName.get(dynamicColumnName)!

      row[dynamicColumnIndex] = row[this.#columnValueFromIndex]

      resultRows.push(row)
    }

    this.push(resultRows)

    done()
  }
}
