import assert from 'node:assert'
import { Transform, TransformCallback, TransformOptions } from 'stream'

export interface PivotHeaderTransformOptions {
  keyColumn: string
  valueColumn: string
}

export class PivotHeaderTransform extends Transform {
  /** Incoming rows buffer */
  #rowsBuffer = [] as unknown[][]

  /** Headers */
  #header!: string[]
  #headersSet!: Set<string>
  #rowLen!: number

  #keyColumnIndex!: number
  #valueColumnIndex!: number

  #keyHeadersSet = new Set<string>()

  constructor(public options: TransformOptions & PivotHeaderTransformOptions) {
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

      this.#keyColumnIndex = this.#header.indexOf(this.options.keyColumn)

      if (this.#keyColumnIndex === -1) {
        // TODO Нужно ли передавать ошибку в done?
        throw new Error(`Column "${this.options.keyColumn}" not found`)
      }

      this.#valueColumnIndex = this.#header.indexOf(this.options.valueColumn)

      if (this.#valueColumnIndex === -1) {
        throw new Error(`Column "${this.options.valueColumn}" not found`)
      }
    }

    for (const row of chunk) {
      assert.equal(row.length, this.#rowLen)

      const pivotColumnName = row[this.#keyColumnIndex]

      if (
        pivotColumnName == null ||
        typeof pivotColumnName !== 'string' ||
        pivotColumnName === ''
      ) {
        throw new Error(`Pivot column name should be not empty string`)
      }

      if (this.#headersSet.has(pivotColumnName)) {
        throw new Error(
          `Pivot column name "${pivotColumnName}"` +
            ' overlap exist header column with same name'
        )
      }

      this.#keyHeadersSet.add(pivotColumnName)
    }

    this.#rowsBuffer.push(...chunk)

    done()
  }

  override _final(done: TransformCallback): void {
    if (this.#rowsBuffer.length === 0) {
      done()
      return
    }

    const pivotHeaders = [...this.#keyHeadersSet.values()]
    const pivotHeadersLen = pivotHeaders.length

    const pivotHeadersIndexByName = new Map(
      [...pivotHeaders.entries()].map(([index, h]) => [
        h,
        index + this.#header.length
      ])
    )

    const resultRows = [[...this.#header, ...pivotHeaders]] as unknown[][]

    for (const row of this.#rowsBuffer) {
      row.push(...Array(pivotHeadersLen))

      const pivotColumnName = row[this.#keyColumnIndex] as string
      const pivotColumnIndex = pivotHeadersIndexByName.get(pivotColumnName)!

      row[pivotColumnIndex] = row[this.#valueColumnIndex]

      resultRows.push(row)
    }

    this.push(resultRows)

    done()
  }
}
