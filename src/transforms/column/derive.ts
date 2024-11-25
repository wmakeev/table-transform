import { TableChunksTransformer, TransformColumnsError } from '../../index.js'
import { TransformExpressionContext } from '../index.js'
import { add } from './add.js'
import { transform } from './transform.js'

export interface DeriveColumnParams {
  /** Column name */
  column: string

  /** Expression */
  expression: string
}

const TRANSFORM_NAME = 'Column:Derive'

export const derive = (
  params: DeriveColumnParams,
  context?: TransformExpressionContext
): TableChunksTransformer => {
  const { column, expression } = params

  return source => {
    if (
      source.getHeader().find(h => !h.isDeleted && h.name === column) !==
      undefined
    ) {
      throw new TransformColumnsError(
        `Can't drive already exist column`,
        TRANSFORM_NAME,
        source.getHeader(),
        [column]
      )
    }

    return transform({ column, expression }, context)(add({ column })(source))
  }
}
