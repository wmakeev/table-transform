import { TransformError } from '../errors/index.js'
import {
  createTableHeader,
  forceArrayLength,
  generateHeaderColumnNames
} from '../tools/index.js'
import {
  ColumnHeader,
  HeaderMode,
  TableChunksSource,
  TableRow,
  TableTransfromConfig
} from '../types/index.js'
import { Context } from './Context.js'

const generateForcedHeader = (
  headerMode: HeaderMode,
  count: number
): ColumnHeader[] => {
  const columnsNames = generateHeaderColumnNames(headerMode, count)

  const header = createTableHeader(columnsNames)

  return header
}

export async function getInitialTableSource(params: {
  chunkedRowsIterable: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
  inputHeaderOptions?: TableTransfromConfig['inputHeader']
  /** Transform context */
  context?: Context | undefined
}): Promise<TableChunksSource> {
  const { inputHeaderOptions, chunkedRowsIterable } = params

  const context = params.context ?? new Context()

  //#region Predefined forced header
  if (
    inputHeaderOptions?.mode !== 'FIRST_ROW' &&
    inputHeaderOptions?.forceColumnsCount != null
  ) {
    if (inputHeaderOptions.forceColumnsCount <= 0) {
      throw new TransformError(
        'inputHeader.forceColumnsCount expected to be greater then 0'
      )
    }

    const header = generateForcedHeader(
      inputHeaderOptions.mode,
      inputHeaderOptions.forceColumnsCount
    )

    return {
      getContext: () => context,
      getHeader: () => header,
      async *[Symbol.asyncIterator]() {
        const forcedLen = inputHeaderOptions.forceColumnsCount!

        for await (const chunk of chunkedRowsIterable) {
          // TODO Аналогичные проверки на массив ниже. Можно/нужно объединить?
          if (!Array.isArray(chunk)) {
            throw new TransformError('Rows chunk expected to be Array')
          }

          if (chunk.length > 0) {
            for (const row of chunk) {
              if (!Array.isArray(row)) {
                throw new TransformError('Row expected to be Array')
              }
              forceArrayLength(row, forcedLen)
            }

            yield chunk
          }
        }
      }
    }
  }
  //#endregion

  /** Generated header will be prepended */
  const shouldHeaderPrepended =
    inputHeaderOptions?.mode != null && inputHeaderOptions.mode !== 'FIRST_ROW'

  /** Incoming data iterator */
  let sourceIterator: Iterator<TableRow[]> | AsyncIterator<TableRow[]>

  if (Symbol.iterator in chunkedRowsIterable) {
    sourceIterator = chunkedRowsIterable[Symbol.iterator]()
  } else if (Symbol.asyncIterator in chunkedRowsIterable) {
    sourceIterator = chunkedRowsIterable[Symbol.asyncIterator]()
  } else {
    throw new TransformError('rowsChunks is not iterable')
  }

  const firstIteratorResult = await sourceIterator.next()

  if (firstIteratorResult.done === true) {
    // TODO Подумать как обработать
    throw new TransformError('Iterator not yields')
  }

  /** First incoming rows chunk */
  let curChunk = firstIteratorResult.value

  if (!Array.isArray(curChunk)) {
    // TODO Добавить подробную ошибку с типом
    throw new TransformError('Expected source to be array')
  }

  /** First row of incoming chunk */
  const firstRow = curChunk[0]

  if (firstRow == null) {
    throw new TransformError('Expected source first row to be array')
  }

  if (firstRow.length === 0) {
    throw new TransformError('Source header row is empty')
  }

  /** Header row of incoming data */
  let headerRow: TableRow

  // No header. Header should be generated and prepended.
  if (shouldHeaderPrepended) {
    headerRow = generateHeaderColumnNames(
      inputHeaderOptions.mode,
      firstRow.length // inputHeaderOptions.forceColumnsCount == null
    )
  }

  // Header should exist. Extract header from first row.
  else {
    headerRow = firstRow
    curChunk = curChunk.slice(1)
  }

  // Get chunk without header

  /** Header of incoming data */
  const header = createTableHeader(headerRow)

  /** Length of source data header (columns count) */
  const headerLen = header.length

  const getSourceGenerator = async function* () {
    try {
      while (true) {
        if (!Array.isArray(curChunk))
          throw new TransformError('Rows chunk expected to be Array')

        if (curChunk.length > 0) {
          for (const row of curChunk) {
            if (!Array.isArray(row)) {
              throw new TransformError('Row expected to be Array')
            }

            forceArrayLength(row, headerLen)
          }

          yield curChunk
        }

        const iterResult = await sourceIterator.next()

        if (iterResult.done === true) return iterResult.value

        curChunk = iterResult.value
      }
    } finally {
      // Allow custom error handling to make it work, and then stop the incoming stream. #dhf042pf
      setImmediate(() => {
        // TODO Может вместо setImmediate - await?
        sourceIterator.return?.(undefined)
      })
    }
  }

  return {
    getContext: () => context,
    getHeader: () => header,
    [Symbol.asyncIterator]: getSourceGenerator
  }
}
