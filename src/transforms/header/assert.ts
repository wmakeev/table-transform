import {
  TableChunksTransformer,
  TransformColumnsNotFoundError
} from '../../index.js'

export interface AssertHeaderParams {
  headers: string[]
}

const TRANSFORM_NAME = 'Header:Assert'

/**
 * Asserts headers exist
 */
export const assert = (params: AssertHeaderParams): TableChunksTransformer => {
  return source => {
    const { headers } = params

    const actualHeader = source.getHeader().filter(h => !h.isDeleted)

    const notFound = []

    for (const colHeader of headers) {
      if (actualHeader.findIndex(h => h.name === colHeader) === -1)
        notFound.push(colHeader)
    }

    if (notFound.length > 0) {
      throw new TransformColumnsNotFoundError(
        TRANSFORM_NAME,
        source.getHeader(),
        notFound
      )
    }

    return source
  }
}
