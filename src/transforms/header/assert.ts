import {
  TableChunksTransformer,
  TransformStepColumnsNotFoundError
} from '../../index.js'
import { TransformBaseParams } from '../index.js'

export interface AssertHeaderParams extends TransformBaseParams {
  headers: string[]
}

const TRANSFORM_NAME = 'Header:Assert'

/**
 * Asserts headers exist
 */
export const assert = (params: AssertHeaderParams): TableChunksTransformer => {
  return source => {
    const { headers } = params

    const actualHeader = source.getTableHeader().filter(h => !h.isDeleted)

    const notFound = []

    for (const colHeader of headers) {
      if (actualHeader.findIndex(h => h.name === colHeader) === -1)
        notFound.push(colHeader)
    }

    if (notFound.length > 0) {
      throw new TransformStepColumnsNotFoundError(
        TRANSFORM_NAME,
        source.getTableHeader(),
        notFound
      )
    }

    return source
  }
}
