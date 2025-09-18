import { TableChunksTransformer, TransformColumnsError } from '../../index.js'
import { add } from './add.js'
import { transform } from './transform.js'

export interface DeriveColumnParams {
  /** Column name */
  column: string

  /** Expression */
  expression: string
}

const TRANSFORM_NAME = 'Column:Derive'

export const derive = (params: DeriveColumnParams): TableChunksTransformer => {
  const { column, expression } = params

  return source => {
    if (
      source.getTableHeader().find(h => !h.isDeleted && h.name === column) !==
      undefined
    ) {
      throw new TransformColumnsError(
        `Can't derive already exist column`,
        TRANSFORM_NAME,
        source.getTableHeader(),
        [column]
      )
    }

    return transform({ column, expression })(add({ column })(source))
  }
}
