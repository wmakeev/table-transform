import { TableChunksTransformer } from '../../index.js'
import { compose } from '../index.js'
import { rename, RenameColumnParams } from './rename.js'
import { unroll, UnrollColumnParams } from './unroll.js'

const TRANSFORM_NAME = 'Column:UnrollAndRename'

/**
 * Unroll column and rename
 */
export const unrollAndRename = (
  params: UnrollColumnParams & RenameColumnParams
): TableChunksTransformer => {
  return sourceIn => {
    const { column, arrIndex, strictArrayColumn, newName, name, description } =
      params

    return compose({
      name: name ?? TRANSFORM_NAME,
      description,
      transforms: [
        unroll({
          column,
          arrIndex,
          strictArrayColumn
        }),
        rename({
          column,
          newName
        })
      ]
    })(sourceIn)
  }
}
