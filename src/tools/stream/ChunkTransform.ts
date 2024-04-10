import { Transform, TransformCallback, TransformOptions } from 'stream'

export class ChunkTransform extends Transform {
  #batchSize

  #buffer: unknown[] = []

  constructor(opts?: TransformOptions & { batchSize: number }) {
    super({
      ...opts,
      readableObjectMode: true,
      writableObjectMode: true
    })

    this.#batchSize = opts?.batchSize ?? 1
  }

  override _transform(
    chunk: any,
    _encoding: BufferEncoding,
    done: TransformCallback
  ): void {
    this.#buffer.push(chunk)

    if (this.#buffer.length >= this.#batchSize) {
      this.push(this.#buffer)
      this.#buffer = []
    }

    done()
  }

  override _flush(done: TransformCallback): void {
    if (this.#buffer.length > 0) {
      this.push(this.#buffer)
      this.#buffer = []
    }

    done()
  }
}
