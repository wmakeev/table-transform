import assert from 'assert'
import {
  TransformStepColumnsError,
  TransformStepColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableHeader, TableRow } from '../../index.js'
import { add as addColumn } from './index.js'

const TRANSFORM_NAME = 'Column:Unfold'

export interface UnfoldParams {
  column: string
  fields: string[]
  allowExistColumnsOverlap?: boolean
}

type UnfoldObj = Record<string, unknown> | Array<unknown>

type FiledUnfolder = (
  unfoldItem: UnfoldObj,
  field: string,
  fieldIndex: number,
  row: TableRow,
  rowColIndex: number
) => void

const unfoldObjField: FiledUnfolder = (
  unfoldItem,
  field,
  _,
  row,
  rowColIndex
) => {
  if (Object.hasOwn(unfoldItem, field)) {
    row[rowColIndex] =
      (unfoldItem as Record<string, unknown>)[field] ?? undefined
  }
}

const unfoldArrItem: FiledUnfolder = (
  unfoldItem,
  _,
  fieldIndex: number,
  row: TableRow,
  rowColIndex: number
) => {
  row[rowColIndex] = (unfoldItem as unknown[])[fieldIndex] ?? undefined
}

/**
 * Set object fields to columns
 */
export const unfold = (params: UnfoldParams): TableChunksTransformer => {
  const { column, fields, allowExistColumnsOverlap = false } = params

  if (typeof column !== 'string' || column === '') {
    new TransformStepError('Incorrect column parameter', TRANSFORM_NAME)
  }

  if (
    !Array.isArray(fields) ||
    fields.length === 0 ||
    fields.some(f => typeof f !== 'string')
  ) {
    new TransformStepError('Incorrect fields parameter', TRANSFORM_NAME)
  }

  return source => {
    const tableHeader = source.getTableHeader()

    const columns: TableHeader = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    if (columns.length === 0) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    // TODO Нужно ли работать с колонками массивами в этом методе?
    if (columns.length > 1) {
      throw new TransformStepColumnsError(
        'Array column not supported',
        TRANSFORM_NAME,
        tableHeader,
        [column]
      )
    }

    const firstColumn = columns[0]!

    if (fields.includes(column)) {
      throw new TransformStepColumnsError(
        "Unfolding fields can't contain unfold column name",
        TRANSFORM_NAME,
        tableHeader,
        [column]
      )
    }

    const headerColumns = tableHeader.filter(h => !h.isDeleted).map(h => h.name)

    const intersectedField = fields.filter(f => headerColumns.includes(f))

    if (intersectedField.length > 0 && allowExistColumnsOverlap === false) {
      throw new TransformStepColumnsError(
        'Unfolding fields intersect with exist columns with same name',
        TRANSFORM_NAME,
        tableHeader,
        intersectedField
      )
    }

    let _source = source

    for (const col of fields) {
      _source = addColumn({
        column: col
      })(_source)
    }

    const resultHeader = _source.getTableHeader()

    const headerColumnIndexByName = new Map(
      resultHeader.map(h => [h.name, h.index])
    )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of _source) {
        for (const row of chunk) {
          const unfoldObj = row[firstColumn.index]

          let unfolder: FiledUnfolder

          if (unfoldObj == null) {
            continue
          } else if (Array.isArray(unfoldObj)) {
            unfolder = unfoldArrItem
          } else if (typeof unfoldObj === 'object') {
            unfolder = unfoldObjField
          } else {
            continue
          }

          for (let i = 0, len = fields.length; i < len; i++) {
            const field = fields[i]!
            const colIndex = headerColumnIndexByName.get(field)
            assert.ok(colIndex !== undefined)

            unfolder(unfoldObj as UnfoldObj, field, i, row, colIndex)
          }
        }

        yield chunk
      }
    }

    return {
      ...source,
      getTableHeader: () => resultHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
