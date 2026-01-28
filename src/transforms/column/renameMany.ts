import { TableChunksTransformer } from '../../index.js'
import { TransformBaseParams, compose } from '../index.js'
import { rename } from './rename.js'

const TRANSFORM_NAME = 'Column:RenameMany'

export interface RenameManyColumnParams extends TransformBaseParams {
  renames: Record<string, string>
}

/**
 * Rename many headers
 */
export const renameMany = (
  params: RenameManyColumnParams
): TableChunksTransformer => {
  return sourceIn => {
    const { name, renames } = params

    return compose({
      name: name ?? TRANSFORM_NAME,
      transforms: Object.entries(renames).map(ent =>
        rename({ column: ent[0], newName: ent[1] })
      )
    })(sourceIn)
  }
}
