import { TableChunksTransformer } from '../../index.js'
import { probePut } from './probePut.js'

export interface ProbePutManyColumnParams {
  columns: string[]
}

export const probePutMany = (
  params: ProbePutManyColumnParams
): TableChunksTransformer => {
  const { columns } = params

  return source => {
    let _source = source

    for (const column of columns) {
      _source = probePut({ column })(_source)
    }

    return _source
  }
}
