import { TableChunksTransformer } from '../../index.js'
import { ColumnTransformExpressionParams, compose } from '../index.js'
import { rename, RenameColumnParams } from './rename.js'
import { transform } from './transform.js'

const TRANSFORM_NAME = 'Column:TransformAndRename'

/**
 * Transform column and rename
 */
export const transformAndRename = (
  params: ColumnTransformExpressionParams & RenameColumnParams
): TableChunksTransformer => {
  return sourceIn => {
    const { column, newName, expression, columnIndex, name, description } =
      params

    return compose({
      name: name ?? TRANSFORM_NAME,
      description,
      transforms: [
        transform({
          column,
          expression,
          columnIndex
        }),
        rename({
          column,
          newName
        })
      ]
    })(sourceIn)
  }
}
