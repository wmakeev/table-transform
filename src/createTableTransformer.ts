import assert from 'assert'
import { TransformError } from './errors/index.js'
import {
  ColumnHeader,
  HeaderMode,
  TableChunksSource,
  TableRow,
  TableTransformer,
  TableTransfromConfig,
  forceArrayLength,
  transforms as tf
} from './index.js'
import {
  compareTableRawHeader,
  createTableHeader,
  generateHeaderColumnNames,
  getChunkNormalizer
} from './tools/headers.js'

export async function* getColumnCountForcerGen(
  source: Iterable<TableRow[]> | AsyncIterable<TableRow[]>,
  count: number
): AsyncGenerator<TableRow[]> {
  for await (const chunk of source) {
    for (const row of chunk) forceArrayLength(row, count)
    yield chunk
  }
}

const generateForcedHeader = (
  headerMode: HeaderMode,
  count: number
): ColumnHeader[] => {
  const columnsNames = generateHeaderColumnNames(headerMode, count)

  const header = createTableHeader(columnsNames)

  return header
}

async function getInitialTableSource(params: {
  chunkedRowsIterable: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
  inputHeaderOptions?: TableTransfromConfig['inputHeader']
}): Promise<TableChunksSource> {
  const { inputHeaderOptions, chunkedRowsIterable } = params

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

    return {
      header: generateForcedHeader(
        inputHeaderOptions.mode,
        inputHeaderOptions.forceColumnsCount
      ),
      getSourceGenerator: () =>
        getColumnCountForcerGen(
          chunkedRowsIterable,
          inputHeaderOptions.forceColumnsCount!
        )
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
  let firstChunk = firstIteratorResult.value

  if (firstChunk[0] == null) {
    throw new TransformError('Empty or incorrect rows chunk')
  }

  const firstRow = firstChunk[0]

  if (firstRow.length === 0) {
    throw new TransformError('Source have not any header')
  }

  /** Header row of incoming data */
  let srcHeaderRow: TableRow

  // No header. Header should be generated and prepended.
  if (shouldHeaderPrepended) {
    srcHeaderRow = generateHeaderColumnNames(
      inputHeaderOptions.mode,
      firstRow.length // inputHeaderOptions.forceColumnsCount == null
    )
  }

  // Header should exist. Extract header from first row.
  else {
    srcHeaderRow = firstChunk[0]
    firstChunk = firstChunk.slice(1)
  }

  if (!Array.isArray(srcHeaderRow)) {
    // TODO Добавить подробную ошибку с типом
    throw new TransformError('Expected header row to be array')
  }

  /** Header of incoming data */
  const srcHeader = createTableHeader(srcHeaderRow)

  /** Length of source data header (columns count) */
  const srcHeadeLength = srcHeader.length

  const getSourceGenerator = async function* () {
    try {
      if (firstChunk.length !== 0) {
        yield firstChunk
      }

      firstChunk = []

      while (true) {
        const iterResult = await sourceIterator.next()

        if (iterResult.done === true) return iterResult.value

        const chunk = iterResult.value

        if (!Array.isArray(chunk))
          throw new TransformError('Rows chunk expected to be Array')

        for (const row of chunk) {
          if (!Array.isArray(row)) {
            throw new TransformError('Row expected to be Array')
          }

          forceArrayLength(row, srcHeadeLength)
        }

        yield chunk
      }
    } finally {
      // Allow custom error handling to make it work, and then stop the incoming stream. #dhf042pf
      setImmediate(() => {
        sourceIterator.return?.(undefined)
      })
    }
  }

  return { header: srcHeader, getSourceGenerator }
}

export function createTableTransformer(
  config: TableTransfromConfig
): TableTransformer {
  const { transforms = [], inputHeader, outputHeader, errorHandle } = config

  const errorOutColumns =
    errorHandle && (errorHandle.outputColumns ?? [errorHandle.errorColumn])

  if (errorOutColumns?.length === 0) {
    throw new Error('Error columns not specified')
  }

  return async function* (
    source: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
  ) {
    let tableSource: TableChunksSource | null = null
    let isResultHeaderKnown = false

    try {
      tableSource = await getInitialTableSource({
        inputHeaderOptions: inputHeader,
        chunkedRowsIterable: source
      })

      const initialTableHeader = tableSource.header

      const transforms_ = [
        ...transforms,

        // Ensure all error columns exist
        ...(errorOutColumns != null
          ? errorOutColumns.map(columnName => tf.column.add({ columnName }))
          : []),

        // Ensure all forsed columns exist and select
        ...(outputHeader?.forceColumns
          ? [
              ...outputHeader.forceColumns.map(columnName =>
                tf.column.add({ columnName })
              ),
              tf.column.select({
                columns: [
                  ...new Set([
                    ...outputHeader.forceColumns,
                    ...(errorOutColumns ?? [])
                  ])
                ]
              })
            ]
          : [])
      ]

      // Chain transformations
      for (const transform of transforms_) {
        const result = await transform(tableSource)

        if (result == null) return

        tableSource = result
      }

      const isHeaderChanged = !compareTableRawHeader(
        initialTableHeader,
        tableSource.header
      )

      if (outputHeader?.skip !== true) {
        yield [tableSource.header.filter(h => !h.isDeleted).map(h => h.name)]
      }

      isResultHeaderKnown = tableSource != null // Always true here

      const normalizeRowsChunk = getChunkNormalizer(tableSource.header)

      for await (const rowsChunk of tableSource.getSourceGenerator()) {
        yield isHeaderChanged ? normalizeRowsChunk(rowsChunk) : rowsChunk
      }
    } catch (err) {
      // #dhf042pf
      assert.ok(err instanceof Error)

      // TODO Вероятно нужно ловить только TransformError и наследников

      if (errorHandle == null) throw err

      assert.ok(errorOutColumns != null)

      const sourceResultHeaders = isResultHeaderKnown
        ? tableSource!.header.filter(h => !h.isDeleted).map(h => h.name)
        : [
            ...new Set([
              ...(outputHeader?.forceColumns ?? []),
              ...errorOutColumns
            ])
          ]

      // TODO Сделать специальный класс ошибки
      const errorInfo = {
        name: err.name,
        code: (err as any).code,
        message: err.message
      }

      yield* createTableTransformer({
        transforms: [
          // Add source columns
          ...sourceResultHeaders.map(col =>
            tf.column.add({
              columnName: col,
              force: col !== errorHandle.errorColumn
            })
          ),

          // Custom transforms
          ...(errorHandle.transforms ?? []),

          // Select columns
          tf.column.select({
            columns: sourceResultHeaders
          })
        ],
        outputHeader: {
          skip: isResultHeaderKnown
        }
      })([[[errorHandle.errorColumn], [errorInfo]]])
    }
  }
}
