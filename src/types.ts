/** Column header */
export type ColumnHeader = string

/** Table header */
export type TableHeader = ColumnHeader[]

/** Metadata of column header */
export type ColumnHeaderMeta = {
  /** Column name */
  name: string

  /**
   * Index in source data row.
   *
   * If index is `-1` then column is not mapped to source.
   */
  srcIndex: number
}

/** Metadata of table headers */
export type TableHeaderMeta = ColumnHeaderMeta[]

/** Data row */
export type DataRow = Array<unknown | null>

export type DataRowChunk = DataRow[]

export type DataRowChunkInfo = {
  header: TableHeaderMeta
  rows: DataRowChunk
  rowLength: number
}

/**
 * Table row transformer. Gets batch of rows and returns transformed batch.
 */
export type DataRowChunkTransformer = (
  rowsChunkInfo: DataRowChunkInfo
) => Promise<DataRowChunkInfo>

export type TableTransfromConfig = {
  /** Transformers */
  transforms: DataRowChunkTransformer[]

  /**
   * **Prepended header style.**
   *
   * If option is specified, then autogenerated header row will be prepended to data rows.
   *
   * Options:
   * - `COLUMN_NUM` - add headers like `Col1`, `Col2`, ...
   * - `EXCEL_STYLE` - add headers like `A`, `B`, ..., `AX`, ...
   *
   * Use this option if table has't headers.
   */
  prependHeaders?: 'COLUMN_NUM' | 'EXCEL_STYLE'
}
