import { compareTableHeader } from './compareTableHeader.js'
import {
  ColumnHeaderMeta,
  DataRow,
  DataRowChunk,
  DataRowChunkInfo,
  TableHeaderMeta,
  TableTransfromOptions
} from './index.js'
import {
  generateColumnNumHeader,
  generateExcelStyleHeader
} from './tools/headers.js'

export const createTableTransformer = (config: TableTransfromOptions) => {
  const { transforms, prependHeaders } = config

  const isHeaderPrepended = prependHeaders != null

  return async function* (
    source: Iterable<DataRowChunk> | AsyncIterable<DataRowChunk>
  ) {
    let isHeaderYielded = false
    let isHeaderChanged: boolean | null = null

    // TODO Возможно добавить настройку `emitHeaderMode = 'ALWAYS' | 'OMIT' | 'OMIT_WITHOUT_DATA'`
    const shouldEmitHeader = true

    let srcTableHeaderMeta: TableHeaderMeta | undefined

    for await (let rowsChunk of source) {
      //#region First source chunk of rows
      if (srcTableHeaderMeta === undefined) {
        if (rowsChunk[0] == null) {
          throw new Error('Empty or incorrect rows chunk')
        }

        const firstRow = rowsChunk[0]

        if (firstRow.length === 0) {
          throw new Error('No columns in row')
        }

        let srcHeaderRow: DataRow

        // No header. Header should be generated and prepended.
        if (isHeaderPrepended) {
          srcHeaderRow =
            prependHeaders === 'EXCEL_STYLE'
              ? generateExcelStyleHeader(firstRow.length)
              : generateColumnNumHeader(firstRow.length)
        }

        // Header should exist. Extract header from first row.
        else {
          srcHeaderRow = rowsChunk[0]
          rowsChunk = rowsChunk.slice(1)
        }

        srcTableHeaderMeta = srcHeaderRow.flatMap((h, index) => {
          if (h === '' || h == null) return []

          const colMeta: ColumnHeaderMeta = {
            srcIndex: index,
            name: String(h)
          }

          return colMeta
        })
      }
      //#endregion

      let dataRowChunkInfo: DataRowChunkInfo = {
        header: srcTableHeaderMeta,
        rows: rowsChunk,
        rowLength: srcTableHeaderMeta.length
      }

      // Make chunk transformation
      for (const transform of transforms) {
        dataRowChunkInfo = await transform(dataRowChunkInfo)
      }

      /** Is current transformed chunk empty */
      const isEmptyChunk = dataRowChunkInfo.rows.length === 0

      if (shouldEmitHeader && !isHeaderYielded) {
        const resultHeader = dataRowChunkInfo.header.map(h => h.name)

        yield [resultHeader]

        isHeaderYielded = true
      }

      if (isEmptyChunk) continue

      if (isHeaderChanged === null) {
        isHeaderChanged = !compareTableHeader(
          srcTableHeaderMeta,
          dataRowChunkInfo.header
        )
      }

      // Headers not changed. Rows normalization can be skipped.
      if (!isHeaderChanged) {
        yield dataRowChunkInfo.rows
        continue
      }

      //#region Normalize transformed rows
      const resultRows: DataRowChunk = []

      for (const row of dataRowChunkInfo.rows) {
        const resultRow: DataRow = []

        for (const h of dataRowChunkInfo.header) {
          resultRow.push(row[h.srcIndex])
        }

        resultRows.push(resultRow)
      }

      yield resultRows
      //#endregion
    }
  }
}
