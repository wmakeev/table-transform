import { TableChunksTransformer, TransformColumnsError } from '../../index.js'
import { add, probePut, ProbePutColumnParams } from './index.js'

const TRANSFORM_NAME = 'Column:ProbeRestore'

/**
 * Restore column with early probed value
 */
export const probeRestore = (
  params: ProbePutColumnParams
): TableChunksTransformer => {
  const { column, key = column } = params

  return source => {
    const tableHeader = source.getTableHeader()

    if (
      tableHeader.find(h => !h.isDeleted && h.name === column) !== undefined
    ) {
      throw new TransformColumnsError(
        `Can't restore probe to already exist column`,
        TRANSFORM_NAME,
        source.getTableHeader(),
        [column]
      )
    }

    return probePut({ column, key })(add({ column })(source))
  }
}
