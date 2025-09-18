import { TransformBugError } from '../../errors/index.js'
import {
  TableChunksSource,
  TableChunksTransformer,
  TableHeader
} from '../../index.js'

export interface AddColumnParams {
  column: string

  defaultValue?: unknown

  /**
   * If the adding column(s) already exists, then add a new one with the same
   * name.
   */
  forceArrayColumn?: boolean
}

/**
 * Adds new column
 */
export const add = (params: AddColumnParams): TableChunksTransformer => {
  const defaultValue = params.defaultValue ?? null

  return source => {
    const tableHeader = source.getTableHeader()

    // Skip column adding?
    if (
      params.forceArrayColumn !== true &&
      tableHeader.findIndex(h => !h.isDeleted && h.name === params.column) !==
        -1
    ) {
      return source
    }

    // TODO Скорее всего нет большого смысла переиспользовать удаленные колонки.
    // Если формировать размер строки заранее на основе вывода колонок в дорожке
    // то добавление колонки (без значения по умолчанию) будет прозрачной
    // операцией только на заголовках без необходимости итерации по массиву.

    const firstDeletedHeader = tableHeader.find(h => h.isDeleted)

    // Add a cell at the end of a row or reuse a deleted cell
    const transformedHeader: TableHeader =
      firstDeletedHeader === undefined
        ? [
            ...tableHeader,
            {
              index: tableHeader.length,
              name: params.column,
              isDeleted: false
            }
          ]
        : tableHeader.map(h =>
            h === firstDeletedHeader
              ? {
                  index: firstDeletedHeader.index,
                  name: params.column,
                  isDeleted: false
                }
              : h
          )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of source) {
        chunk.forEach(row => {
          if (firstDeletedHeader === undefined) {
            row.push(defaultValue)
          } else {
            row[firstDeletedHeader.index] = defaultValue
          }

          if (row.length !== transformedHeader.length) {
            throw new TransformBugError('Row length not satisfies header')
          }
        })

        yield chunk
      }
    }

    const resultChunk: TableChunksSource = {
      ...source,
      getTableHeader: () => transformedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }

    return resultChunk
  }
}
