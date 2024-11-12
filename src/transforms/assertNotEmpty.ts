import { TransformAssertError } from '../errors/index.js'
import { TableChunksTransformer } from '../index.js'

const TRANSFORM_NAME = 'AssertNotEmpty'

/**
 * Asserts stream is not empty
 */
export const assertNotEmpty = (): TableChunksTransformer => {
  return source => {
    return {
      ...source,
      getHeader: () => source.getHeader(),
      async *[Symbol.asyncIterator]() {
        let isEmpty = true

        for await (const chunk of source) {
          yield chunk
          isEmpty = false
        }

        if (isEmpty) {
          throw new TransformAssertError('Data stream is empty', TRANSFORM_NAME)
        }
      }
    }
  }
}
