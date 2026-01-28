import {
  TransformStepColumnsError,
  TransformStepColumnsNotFoundError,
  TransformStepError
} from '../../errors/index.js'
import { TableChunksTransformer, TableHeader } from '../../index.js'
import { TransformBaseParams } from '../index.js'
import { add as addColumn, remove as removeColumn } from './index.js'

const TRANSFORM_NAME = 'Column:Unfold'

export interface UnfoldParams extends TransformBaseParams {
  /**
   * Колонка которая содержит данные для разворачивания в отдельные колонки
   */
  column: string

  /**
   * Наименования клонок в которые будут развернуты значения из соотв. полей
   * значения в колонке `column`.
   */
  fields: string[]

  /**
   * Удалить колонку `column` после раскрытия
   */
  removeColumn?: boolean
}

/**
 * Set object fields to columns
 */
export const unfold = (params: UnfoldParams): TableChunksTransformer => {
  const { column, fields, removeColumn: removeSrcColumn = false } = params

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

    const tableHeaders: TableHeader = tableHeader.filter(
      h => !h.isDeleted && h.name === column
    )

    if (tableHeaders.length === 0) {
      throw new TransformStepColumnsNotFoundError(TRANSFORM_NAME, tableHeader, [
        column
      ])
    }

    // TODO Нужно ли работать с колонками массивами в этом методе?
    if (tableHeaders.length > 1) {
      throw new TransformStepColumnsError(
        'Array column not supported',
        TRANSFORM_NAME,
        tableHeader,
        [column]
      )
    }

    const header = tableHeaders[0]!

    let _source = source

    for (const col of fields) {
      _source = addColumn({
        column: col
      })(_source)
    }

    const resultHeader = _source.getTableHeader()

    const fieldsHeadersIndexes = fields.map(
      field =>
        resultHeader.find(h => h.isDeleted === false && h.name === field)!.index
    )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of _source) {
        for (const row of chunk) {
          const unfoldItem = row[header.index]

          // Развернуть массив
          if (Array.isArray(unfoldItem)) {
            for (let i = 0, len = fields.length; i < len; i++) {
              row[fieldsHeadersIndexes[i]!] = (unfoldItem as unknown[])[i]
            }

            continue
          }

          // Развернуть объект
          if (unfoldItem !== null && typeof unfoldItem === 'object') {
            for (let i = 0, len = fields.length; i < len; i++) {
              const field = fields[i]!
              if (Object.hasOwn(unfoldItem, field)) {
                row[fieldsHeadersIndexes[i]!] = (
                  unfoldItem as Record<string, unknown>
                )[field]
              } else {
                row[fieldsHeadersIndexes[i]!] = undefined
              }
            }

            continue
          }

          // Заполнить пустые значения
          for (let i = 0, len = fields.length; i < len; i++) {
            row[fieldsHeadersIndexes[i]!] = undefined
          }
        }

        yield chunk
      }
    }

    return {
      ...(removeSrcColumn ? removeColumn({ column })(_source) : _source),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
