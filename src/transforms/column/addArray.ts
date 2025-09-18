import { TransformStepError } from '../../errors/index.js'
import { TableChunksTransformer } from '../../index.js'
import { add, remove } from './index.js'

const TRANSFORM_NAME = 'Column:AddArray'

export interface AddArrayColumnParams {
  column: string
  length: number
  forceLength?: boolean
  defaultValue?: unknown
}

/**
 * Adds array column
 */
export const addArray = (
  params: AddArrayColumnParams
): TableChunksTransformer => {
  const { column, length, forceLength = false, defaultValue } = params

  if (!Number.isInteger(length) || length <= 0) {
    throw new TransformStepError(
      `Array length expected to be positive number - ${length}`,
      TRANSFORM_NAME
    )
  }

  return source => {
    const tableHeader = source.getTableHeader()

    const existColumns = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    // Array column just exist
    if (
      existColumns.length === length ||
      (forceLength === false && existColumns.length > length)
    ) {
      return source
    }

    let _source = source

    // Columns count is less than expected
    if (existColumns.length < length) {
      const columnsCountToAdd = length - existColumns.length

      for (let i = 0; i < columnsCountToAdd; i++) {
        _source = add({
          column,
          defaultValue,
          forceArrayColumn: true
        })(_source)
      }
    }

    // Columns count exceeds expected
    else {
      for (let i = existColumns.length - 1; i >= length; i--) {
        _source = remove({
          column: column,
          colIndex: existColumns[i]!.index,
          isInternalIndex: true
        })(_source)
      }
    }

    return _source
  }
}
