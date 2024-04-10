import { Transform, TransformCallback, TransformOptions } from 'stream'

export class FlattenTransform extends Transform {
  constructor(opts?: TransformOptions & { batchSize: number }) {
    super({
      ...opts,
      readableObjectMode: true,
      writableObjectMode: true
    })
  }

  override _transform(
    chunk: any[],
    _encoding: BufferEncoding,
    done: TransformCallback
  ): void {
    if (!Array.isArray(chunk)) {
      this.push(chunk)
      done()
      return
    }

    this.cork()

    for (const item of chunk) {
      this.push(item)
    }

    this.uncork()

    done()
  }
}
