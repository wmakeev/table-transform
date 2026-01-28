import { TableChunksTransformer } from '../../index.js'
import { TransformBaseParams } from '../index.js'
import { probeRestore } from './probeRestore.js'

export interface ProbeRestoreManyColumnParams extends TransformBaseParams {
  columns: string[]
}

export const probeRestoreMany = (
  params: ProbeRestoreManyColumnParams
): TableChunksTransformer => {
  const { columns } = params

  return source => {
    let _source = source

    for (const column of columns) {
      _source = probeRestore({ column })(_source)
    }

    return _source
  }
}
