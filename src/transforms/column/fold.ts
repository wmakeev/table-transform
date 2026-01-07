import { TransformStepError } from '../../errors/index.js'
import { TableChunksTransformer } from '../../index.js'
import { add as addColumn, remove as removeColumn } from './index.js'

const TRANSFORM_NAME = 'Column:Fold'

export interface FoldParams {
  /**
   * Наименование сворачиваемых колонок
   */
  columns: string[]

  /** Колонка с результатом */
  targetColumn: string

  /**
   * Сворачивать колонки в массив?
   */
  foldToArray?: boolean

  /**
   * Удалить свернутые колонки
   */
  removeColumns?: boolean
}

/**
 * Set object fields to columns
 */
export const fold = (params: FoldParams): TableChunksTransformer => {
  const {
    columns,
    targetColumn,
    foldToArray = false,
    removeColumns = false
  } = params

  if (typeof targetColumn !== 'string' || targetColumn === '') {
    new TransformStepError('Incorrect targetColumn parameter', TRANSFORM_NAME)
  }

  if (
    !Array.isArray(columns) ||
    columns.length === 0 ||
    columns.some(f => typeof f !== 'string')
  ) {
    new TransformStepError('Incorrect columns parameter', TRANSFORM_NAME)
  }

  if (columns.includes(targetColumn)) {
    new TransformStepError(
      `Folding columns names can't contain target column name`,
      TRANSFORM_NAME
    )
  }

  return source => {
    /** Source with new column */
    const _source1 = addColumn({
      column: targetColumn
    })(source)

    const resultHeader = _source1.getTableHeader()

    const columnsHeadersIndexes = columns.map(
      field =>
        resultHeader.find(h => h.isDeleted === false && h.name === field)!.index
    )

    const targetColHeadIndex = resultHeader.find(
      h => h.isDeleted === false && h.name === targetColumn
    )!.index

    async function* getTransformedSourceGenerator() {
      for await (const chunk of _source1) {
        for (const row of chunk) {
          if (foldToArray) {
            row[targetColHeadIndex] = columnsHeadersIndexes.map(i => row[i])
          } else {
            row[targetColHeadIndex] = Object.fromEntries(
              columns.map((c, index) => [c, row[columnsHeadersIndexes[index]!]])
            )
          }
        }

        yield chunk
      }
    }

    /** Source with removed column */
    let _source2 = _source1

    if (removeColumns) {
      for (const col of columns) {
        _source2 = removeColumn({
          column: col
        })(_source2)
      }
    }

    return {
      ..._source2,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
