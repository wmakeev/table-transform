import assert from 'assert'
import {
  TransformColumnsError,
  TransformColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import {
  isObjectGuard,
  TableChunksTransformer,
  TableHeader
} from '../../index.js'
import { add as addColumn } from './index.js'

const TRANSFORM_NAME = 'Column:Unnest'

export interface UnnestParams {
  column: string
  fields: string[]
}

/**
 * Unnest object fields to columns
 */
export const unnest = (params: UnnestParams): TableChunksTransformer => {
  const { column, fields } = params

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

    const unnestColumns: TableHeader = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    // TODO Нужно ли работать с колонками массивами в этом методе?
    if (unnestColumns.length > 1) {
      new TransformColumnsError(
        'Array column not supported',
        TRANSFORM_NAME,
        tableHeader,
        [column]
      )
    }

    assert.ok(unnestColumns[0])

    const unnestColumn = unnestColumns[0]

    if (fields.includes(column)) {
      new TransformColumnsError(
        "Unnested fields can't contain unnest column name",
        TRANSFORM_NAME,
        tableHeader,
        [column]
      )
    }

    const headerColumns = tableHeader.filter(h => !h.isDeleted).map(h => h.name)

    const intersectedField = fields.filter(f => headerColumns.includes(f))

    if (intersectedField.length > 0) {
      new TransformColumnsError(
        'Unnested fields intersect with exist columns with same name',
        TRANSFORM_NAME,
        tableHeader,
        intersectedField
      )
    }

    if (unnestColumns.length === 0) {
      new TransformColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [column])
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
          const unnestObj = row[unnestColumn.index]

          if (!isObjectGuard(unnestObj)) continue

          for (const field of fields) {
            const index = headerColumnIndexByName.get(field)
            assert.ok(index !== undefined)

            if (Object.hasOwn(unnestObj, field)) {
              row[index] = (unnestObj as Record<string, unknown>)[field] ?? null
            }
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
