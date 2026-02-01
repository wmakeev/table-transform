import { TableChunksTransformer } from '../../index.js'
import { TransformBaseParams, compose } from '../index.js'
import { add } from './add.js'

const TRANSFORM_NAME = 'Column:AddMany'

export interface AddManyColumnParams extends TransformBaseParams {
  columns: string[]
  defaultValue?: unknown
}

/**
 * Add many columns
 */
export const addMany = (
  params: AddManyColumnParams
): TableChunksTransformer => {
  return sourceIn => {
    const { name, columns, defaultValue } = params

    return compose({
      name: name ?? TRANSFORM_NAME,
      transforms: columns.map(col => add({ column: col, defaultValue }))
    })(sourceIn)
  }
}
