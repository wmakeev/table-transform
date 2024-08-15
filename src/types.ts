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

/** Data row */
export type TableRow = Array<unknown | null>

export interface TableChunksAsyncIterable extends AsyncIterable<TableRow[]> {
  getHeader: () => ColumnHeader[]
  [Symbol.asyncIterator]: () => AsyncGenerator<TableRow[]>
}

/**
 * Table row transformer. Gets batch of rows and returns transformed batch.
 */
export type TableChunksTransformer = (
  rowsChunkInfo: TableChunksAsyncIterable
) => TableChunksAsyncIterable

export interface TableTransfromConfig {
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
}

export type HeaderMode = NonNullable<
  TableTransfromConfig['inputHeader']
>['mode']

export type TableTransformer = (
  source: Iterable<TableRow[]> | AsyncIterable<TableRow[]>
) => AsyncGenerator<TableRow[]>

export type SourceProvider = (
  header: ColumnHeader[],
  row: TableRow
) => AsyncGenerator<TableRow[]>
