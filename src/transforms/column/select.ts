import assert from 'assert'
import {
  TransformBugError,
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'
import { add, remove } from './index.js'

const TRANSFORM_NAME = 'Column:Select'

export interface SelectColumnsParams {
  /** Columns that should be selected */
  columns: string[]
  addMissingColumns?: boolean
}

// TODO Не нравится как написан этот модуль. Постоянные переборы массивов.
// .. не критично для этого кода, но может быть можно сделать более красиво.

export const select = (params: SelectColumnsParams): TableChunksTransformer => {
  const { columns: selectedColumns, addMissingColumns = false } = params

  if (selectedColumns.length === 0) {
    throw new TransformStepError(
      'Columns to select not specified',
      TRANSFORM_NAME
    )
  }

  return source => {
    let _source = source

    //#region Ensure all selected headers exist
    const headersSet = new Set(_source.getHeader().filter(h => !h.isDeleted))

    const notFoundColumnsSet = new Set<string>()

    for (const col of selectedColumns) {
      const existHeader = [...headersSet].find(h => h.name === col)

      if (existHeader === undefined) {
        if (addMissingColumns) {
          _source = add({ columnName: col, force: true })(_source)
        } else {
          // TODO В случае выборки колонок массивов может быть не понятно, если
          // не указать что не найдена какая-либо n-ная колонка.
          notFoundColumnsSet.add(col)
        }
      } else {
        headersSet.delete(existHeader)
      }
    }

    if (notFoundColumnsSet.size !== 0) {
      throw new TransformColumnsNotFoundError(
        TRANSFORM_NAME,
        source.getHeader(),
        [...notFoundColumnsSet.values()]
      )
    }
    //#endregion

    const header = _source.getHeader()

    const columnHeaderSet = new Set(header)

    const selected: ColumnHeader[] = []

    for (const col of selectedColumns) {
      const colHeader = [...columnHeaderSet.values()].find(
        h => !h.isDeleted && h.name === col
      )

      if (colHeader == null) {
        throw new TransformBugError('colHeader should be found')
      }

      selected.push(colHeader)
      columnHeaderSet.delete(colHeader)
    }

    const deleted = [...columnHeaderSet.values()]

    // Remove other columns
    for (const h of deleted) {
      if (h.isDeleted) continue
      _source = remove({
        columnName: h.name,
        colIndex: h.index,
        isInternalIndex: true
      })(_source)
    }

    const unorderedHeader = _source.getHeader()

    const resultHeader = [
      ...selected.map(h1 => unorderedHeader.find(h2 => h2.index === h1.index)),
      ...deleted.map(h1 => unorderedHeader.find(h2 => h2.index === h1.index))
    ]

    assert.ok(unorderedHeader.length === resultHeader.length)
    assert.ok(resultHeader.every(h => h != null))

    return {
      getHeader() {
        return resultHeader
      },
      [Symbol.asyncIterator]: _source[Symbol.asyncIterator]
    }
  }
}
