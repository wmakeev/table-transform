import { TableChunksTransformer } from '../index.js'

export interface ComposeParams {
  /** Short name of compose */
  name?: string

  /** Compose description */
  description?: string

  /** Composed transforms */
  transforms: TableChunksTransformer[]
}

// const TRANSFORM_NAME = 'Compose'

/**
 * Compose
 */
export const compose = (params: ComposeParams): TableChunksTransformer => {
  const { transforms } = params

  return source => {
    let source_ = source

    // Chain transformations
    for (const transform of transforms) {
      source_ = transform(source_)
    }

    return source_
  }
}
