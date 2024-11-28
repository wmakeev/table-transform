import { TableChunksTransformer } from '../../index.js'
import { probeTake } from './probeTake.js'

export interface ProbeTakeManyColumnParams {
  columns: string[]
}

export const probeTakeMany = (
  params: ProbeTakeManyColumnParams
): TableChunksTransformer => {
  const { columns } = params

  return source => {
    let _source = source

    for (const column of columns) {
      _source = probeTake({ column })(_source)
    }

    return _source
  }
}
