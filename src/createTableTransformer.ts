import { compareTableHeader } from './compareTableHeader.js'
import {
  ColumnHeader,
  PrependHeadersStyle,
  TableChunksSource,
  TableRow,
  TableTransfromConfig
} from './index.js'
import {
  generateColumnNumHeader,
  generateExcelStyleHeader
} from './tools/headers.js'

const getInitialTableSource = async (params: {
  prependHeaders?: PrependHeadersStyle | undefined
  chunkedRowsIterable: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
}): Promise<TableChunksSource> => {
  const { prependHeaders, chunkedRowsIterable } = params

  const isHeaderPrepended = prependHeaders != null

  let sourceIterator: Iterator<TableRow[]> | AsyncIterator<TableRow[]>

  if (Symbol.iterator in chunkedRowsIterable) {
    sourceIterator = chunkedRowsIterable[Symbol.iterator]()
  } else if (Symbol.asyncIterator in chunkedRowsIterable) {
    sourceIterator = chunkedRowsIterable[Symbol.asyncIterator]()
  } else {
    throw new Error('rowsChunks is not iterable')
  }

  const firstIteratorResult = await sourceIterator.next()

  if (firstIteratorResult.done === true) {
    // TODO Подумать как обработать
    throw new Error('Iterator not yields')
  }

  let firstChunk = firstIteratorResult.value

  if (firstChunk[0] == null) {
    throw new Error('Empty or incorrect rows chunk')
  }

  const firstRow = firstChunk[0]

  if (firstRow.length === 0) {
    throw new Error('No columns in row')
  }

  let srcHeaderRow: TableRow

  // No header. Header should be generated and prepended.
  if (isHeaderPrepended) {
    srcHeaderRow =
      prependHeaders === 'EXCEL_STYLE'
        ? generateExcelStyleHeader(firstRow.length)
        : generateColumnNumHeader(firstRow.length)
  }

  // Header should exist. Extract header from first row.
  else {
    srcHeaderRow = firstChunk[0]
    firstChunk = firstChunk.slice(1)
  }

  const srcTableHeaderMeta: ColumnHeader[] = srcHeaderRow.flatMap(
    (h, index) => {
      if (h === '' || h == null) return []

      const colMeta: ColumnHeader = {
        index: index,
        name: String(h),
        isDeleted: false
      }

      return colMeta
    }
  )

  const rowsChunksNext = async function* () {
    yield firstChunk

    while (true) {
      const iterResult = await sourceIterator.next()

      if (iterResult.done) return iterResult.value

      yield iterResult.value
    }
  }

  return { header: srcTableHeaderMeta, getSourceGenerator: rowsChunksNext }
}

export const createTableTransformer = (config: TableTransfromConfig) => {
  const { transforms, prependHeaders } = config

  return async function* (
    source: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
  ) {
    // TODO Возможно добавить настройку `emitHeaderMode = 'ALWAYS' | 'OMIT' | 'OMIT_WITHOUT_DATA'`
    const shouldEmitHeader = true

    let tableSource = await getInitialTableSource({
      prependHeaders,
      chunkedRowsIterable: source
    })

    const initialTableHeader = tableSource.header

    // Chain transformations
    for (const transform of transforms) {
      tableSource = await transform(tableSource)
    }

    const isHeaderChanged = !compareTableHeader(
      initialTableHeader,
      tableSource.header
    )

    if (shouldEmitHeader) {
      yield [tableSource.header.filter(h => !h.isDeleted).map(h => h.name)]
    }

    for await (const rowsChunk of tableSource.getSourceGenerator()) {
      // Header is changed should normalize result
      if (isHeaderChanged) {
        const normalizedRowsChunk: TableRow[] = []

        for (const row of rowsChunk) {
          const resultRow: TableRow = []

          for (const h of tableSource.header) {
            if (!h.isDeleted) resultRow.push(row[h.index])
          }

          normalizedRowsChunk.push(resultRow)
        }

        yield normalizedRowsChunk
      }

      // Header not changed, pass rows as is
      else {
        yield rowsChunk
      }
    }
  }
}
