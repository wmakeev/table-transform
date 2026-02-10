import {
  TransformStepColumnsNotFoundError,
  TransformStepError,
  TransformStepParameterError
} from '../../errors/index.js'
import {
  ColumnHeader,
  findActiveHeader,
  findActiveHeaders,
  TableChunksTransformer
} from '../../index.js'
import { TransformBaseParams } from '../index.js'
import { addMany, remove as removeColumn } from './index.js'

const TRANSFORM_NAME = 'Column:Unfold'

export interface UnfoldParams extends TransformBaseParams {
  /**
   * Колонка которая содержит данные для разворачивания в отдельные колонки
   */
  column: string

  /**
   * Наименования клонок в которые будут развернуто содержимое колонки `column`.
   *
   * Если `fields` содержит `null` элементы, то элемент массива с соотв. индексом,
   * при разворачивании массива, игнорируется.
   *
   * Если `column` содержит объект, то поля объекта соответствующие значениям
   * `fields` копируются в одноименные колонки.
   *
   * Если `column` содержит массив, то элементы массива копируются в колонки
   * указанные в `fields` в порядке из следования.
   *
   * Требуется заполнить хотя-бы один из параметров `fields` или `fieldsMap`.
   */
  fields?: (string | null)[]

  /**
   * Сопоставление наименования поля объекта из колонки column и наименование
   * колонки куда будет скопировано значение этого поля.
   *
   * Можно использовать совместно с полем `fields` (сопоставление в поле
   * `fieldsMap` имеет больший приоритет).
   *
   * Параметр `fieldsMap` применяется только к элементам с типом объект. При
   * разворачивании массива игнорируется.
   *
   * Требуется заполнить хотя-бы один из параметров `fields` или `fieldsMap`.
   */
  fieldsMap?: Record<string, string>

  /**
   * Удалить колонку `column` после раскрытия
   */
  removeColumn?: boolean
}

/**
 * Set object fields to columns
 */
export const unfold = (params: UnfoldParams): TableChunksTransformer => {
  const {
    column,
    fields = [],
    fieldsMap = {},
    removeColumn: removeSrcColumn = false
  } = params

  if (typeof column !== 'string' || column === '') {
    new TransformStepError('Incorrect column parameter', TRANSFORM_NAME)
  }

  const fieldToColumnNameMap = new Map([
    ...fields.flatMap(it => (it != null ? [[it, it] as [string, string]] : [])),
    ...Object.entries(fieldsMap)
  ])

  const fieldsMapEntries = [...fieldToColumnNameMap.entries()]

  const emptyMappings = fieldsMapEntries.filter(ent => ent[1] == null)

  if (emptyMappings.length > 0) {
    throw new TransformStepParameterError(
      'Incorrect mapping to null',
      TRANSFORM_NAME,
      'fieldsMap',
      JSON.stringify(Object.fromEntries(emptyMappings))
    )
  }

  if (fieldToColumnNameMap.size === 0) {
    new TransformStepError('No fields to columns map specified', TRANSFORM_NAME)
  }

  const sourceMappedColumnsNames = [...fieldToColumnNameMap.keys()]
  const targetMappedColumnsNames = [...fieldToColumnNameMap.values()]

  return sourceIn => {
    const inTableHeader = sourceIn.getTableHeader()

    const unfoldColHeader: ColumnHeader | null = findActiveHeader(
      column,
      inTableHeader
    )

    const _source = addMany({
      name: `add unfold columns`,
      columns: targetMappedColumnsNames
    })(sourceIn)

    const resultHeader = _source.getTableHeader()

    /**
     * Indexes of `fields` parameter fields (Object and Array unfold)
     */
    const fieldsHeadersIndexes = fields.flatMap((field, index) => {
      return field == null
        ? []
        : [
            [
              index,
              findActiveHeader(
                Object.hasOwn(fieldsMap, field) ? fieldsMap[field]! : field,
                resultHeader
              )!.index
            ] as [number, number]
          ]
    })

    /**
     * Indexes of `fields` + `fieldsMap` parameter fields (only Object unfold)
     */
    const mappedHeadersIndexes = findActiveHeaders(
      targetMappedColumnsNames,
      resultHeader
    ).map(it => it!.index)

    async function* getTransformedSourceGenerator() {
      if (unfoldColHeader == null) {
        throw new TransformStepColumnsNotFoundError(
          TRANSFORM_NAME,
          inTableHeader,
          [column]
        )
      }

      const unfoldColHeaderIndex = unfoldColHeader.index

      for await (const chunk of _source) {
        for (const row of chunk) {
          const unfoldItem = row[unfoldColHeaderIndex]

          // Развернуть массив
          if (Array.isArray(unfoldItem)) {
            for (const fieldHeaderIndexes of fieldsHeadersIndexes) {
              row[fieldHeaderIndexes[1]] = (unfoldItem as unknown[])[
                fieldHeaderIndexes[0]
              ]
            }

            continue
          }

          // Развернуть объект
          if (unfoldItem !== null && typeof unfoldItem === 'object') {
            for (let i = 0, len = mappedHeadersIndexes.length; i < len; i++) {
              const field = sourceMappedColumnsNames[i]!
              if (Object.hasOwn(unfoldItem, field)) {
                row[mappedHeadersIndexes[i]!] = (
                  unfoldItem as Record<string, unknown>
                )[field]
              } else {
                row[mappedHeadersIndexes[i]!] = undefined
              }
            }

            continue
          }

          // Заполнить пустые значения
          for (const i of mappedHeadersIndexes) {
            row[i] = undefined
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
