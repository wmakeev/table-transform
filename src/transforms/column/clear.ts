import { TableChunksTransformer } from '../../index.js'
import { fill } from './fill.js'

// const TRANSFORM_NAME = 'Column:Clear'

export interface ClearColumnParams {
  column: string
  arrIndex?: number
}

/**
 * Clear column content
 */
export const clear = (params: ClearColumnParams): TableChunksTransformer => {
  return source => {
    return fill({
      ...params,
      value: null
    })(source)
  }
}
