import { TransformBugError } from '../../errors/index.js'
import {
  ColumnHeader,
  TableChunksSource,
  TableChunksTransformer
} from '../../index.js'

export interface AddColumnParams {
  column: string

  defaultValue?: unknown

  // TODO force неоднозначен. Можно понимать как принудительную инициализацию колонки.
  /**
   * If the adding column(s) already exists, then add a new one with the same
   * name.
   */
  force?: boolean
}

/**
 * Adds new column
 */
export const add = (params: AddColumnParams): TableChunksTransformer => {
  const defaultValue = params.defaultValue ?? null

  return source => {
    const header = source.getHeader()

    // Skip column adding?
    if (
      params.force !== true &&
      header.findIndex(h => !h.isDeleted && h.name === params.column) !== -1
    ) {
      return source
    }

    // TODO Скорее всего нет большого смысла переиспользовать удаленные колонки.
    // Если формировать размер строки заранее на основе вывода колонок в дорожке
    // то добавление колонки (без значения по умолчанию) будет прозрачной
    // операцией только на заголовках без необходимости итерации по массиву.

    const firstDeletedHeader = header.find(h => h.isDeleted)

    // Add a cell at the end of a row or reuse a deleted cell
    const transformedHeader: ColumnHeader[] =
      firstDeletedHeader === undefined
        ? [
            ...header,
            {
              index: header.length,
              name: params.column,
              isDeleted: false
            }
          ]
        : header.map(h =>
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
      getHeader: () => transformedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }

    return resultChunk
  }
}
