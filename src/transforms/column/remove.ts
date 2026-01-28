import {
  TransformStepColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableHeader } from '../../index.js'
import { TransformBaseParams } from '../index.js'

const TRANSFORM_NAME = 'Column:Remove'

export interface RemoveColumnParams extends TransformBaseParams {
  column: string
  colIndex?: number
  isInternalIndex?: boolean
}

// TODO #hyssd6e Нужно очищать колонку перед/после удаления
// Но параметр isInternalIndex не универсальный и его не передать в clear.
// Требуется общий селектор колонок иначе преумножаются сложные параметры.

/**
 * Remove column
 */
export const remove = (params: RemoveColumnParams): TableChunksTransformer => {
  return source => {
    const { column, colIndex, isInternalIndex = false } = params

    if (isInternalIndex === true && colIndex == null) {
      throw new TransformStepError(
        'isInternalIndex is true, but colIndex is not specified',
        TRANSFORM_NAME
      )
    }

    const deletedColsSrcIndexes: number[] = []

    let headerIndex = 0

    const srcHeader = source.getTableHeader()

    const transformedHeader: TableHeader = srcHeader.flatMap(h => {
      if (!h.isDeleted && h.name === column) {
        if (
          colIndex != null
            ? colIndex === (isInternalIndex ? h.index : headerIndex)
            : true
        ) {
          deletedColsSrcIndexes.push(h.index)

          return {
            ...h,
            isDeleted: true
          }
        }

        headerIndex++
      }

      return h
    })

    if (deletedColsSrcIndexes.length === 0) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, srcHeader, [
        column
      ])
    }

    return {
      ...source,
      getTableHeader: () => transformedHeader,
      [Symbol.asyncIterator]: source[Symbol.asyncIterator]
    }
  }
}
