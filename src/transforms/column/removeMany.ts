import { TableChunksTransformer } from '../../index.js'
import { TransformBaseParams, compose } from '../index.js'
import { remove } from './remove.js'

const TRANSFORM_NAME = 'Column:RemoveMany'

export interface RemoveManyColumnParams extends TransformBaseParams {
  columns: string[]
}

/**
 * Remove many columns
 */
export const removeMany = (
  params: RemoveManyColumnParams
): TableChunksTransformer => {
  return sourceIn => {
    const { name, columns } = params

    return compose({
      name: name ?? TRANSFORM_NAME,
      transforms: columns.map(col => remove({ column: col }))
    })(sourceIn)
  }
}
