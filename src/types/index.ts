import { Context } from '../table-transformer/Context.js'

export * from './guards.js'
export * from './ExpressionContext.js'
export * from './ExpressionCompileProvider.js'

/** Metadata of column header */
export type ColumnHeader = {
  /** Column name */
  name: string

  /**
   * Index in data row.
   */
  index: number

  isDeleted: boolean
}

/** List of columns headers */
export type TableHeader = ColumnHeader[]

/** Data row */
export type TableRow = Array<unknown>

export type TableChunksIterable =
  | Iterable<TableRow[]>
  | AsyncIterable<TableRow[]>

export interface TableChunksSource extends AsyncIterable<TableRow[]> {
  // getPath: () => string[]
  getTableHeader(): TableHeader
  getContext(): Context
}

/**
 * Table row transformer. Gets batch of rows and returns transformed batch.
 */
export type TableChunksTransformer = (
  tableChunksSource: TableChunksSource
) => TableChunksSource

export interface TableTransformConfig {
  /** Transformers */
  transforms?: TableChunksTransformer[] | undefined

  inputHeader?:
    | {
        /**
         * **Header style.**
         *
         * Options:
         * - `FIRST_ROW` - headers come from first row
         * - `COLUMN_NUM` - add headers like `Col1`, `Col2`, ...
         * - `EXCEL_STYLE` - add headers like `A`, `B`, ..., `AX`, ...
         *
         * default: `FIRST_ROW`
         */
        mode: 'FIRST_ROW'

        trimHeaderNames?: boolean | undefined
      }
    | {
        mode: 'COLUMN_NUM' | 'EXCEL_STYLE'

        /**
         * If source columns count ..
         * - less then `forceColumnsCount`, the missing columns will be added
         * - more then `forceColumnsCount`, the extra columns will be deleted
         */
        forceColumnsCount?: number
      }

  outputHeader?: {
    // TODO Причина добавления forceColumns?
    // - унификация select'а
    // - учитывается в errorHandle в отсутствии заголовка

    forceColumns?: string[]

    /** Should skip emit header to result data (default: `false`) */
    skip?: boolean
  }

  errorHandle?: {
    /**
     * Column name in that error will be placed and can be accessed in
     * error handler transform.
     */
    errorColumn: string

    /**
     * Error handler transform
     */
    transforms?: TableChunksTransformer[]
  }

  /** Transform context */
  context?: Context | undefined
}

export type HeaderMode = NonNullable<
  TableTransformConfig['inputHeader']
>['mode']

export type TableTransformer = (
  source: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
) => AsyncGenerator<TableRow[]>

export type HeaderChunkTuple = [header: TableHeader, chunk: TableRow[]]

export type TableRowFlatMapper = (
  header: TableHeader,
  row: TableRow
) => AsyncGenerator<TableRow[]>

export type TableChunksReducer = (source: TableChunksSource) => {
  outputColumns: string[]
  getResult(): Promise<TableRow>
}
