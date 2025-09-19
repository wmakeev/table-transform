import { TransformStepAssertError } from '../errors/index.js'
import { TableChunksTransformer } from '../index.js'

const TRANSFORM_NAME = 'AssertNotEmpty'

/**
 * Asserts stream is not empty
 */
export const assertNotEmpty = (): TableChunksTransformer => {
  return source => {
    return {
      ...source,
      getTableHeader: () => source.getTableHeader(),
      async *[Symbol.asyncIterator]() {
        let isEmpty = true

        for await (const chunk of source) {
          yield chunk
          isEmpty = false
        }

        if (isEmpty) {
          throw new TransformStepAssertError(
            'Data stream is empty',
            TRANSFORM_NAME
          )
        }
      }
    }
  }
}
