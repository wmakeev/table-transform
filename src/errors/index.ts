import { getRowRecord } from '../tools/index.js'
import { ColumnHeader, TableRow } from '../types.js'

export interface ErrorWithReporter {
  report: () => void
}

export class TransformError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = this.constructor.name
  }
}

export class TransformBugError extends TransformError {
  constructor(message: string, options?: ErrorOptions) {
    super(`[BUG] "${message}"`, options)
    this.name = this.constructor.name
  }
}

export class TransformStepError extends TransformError {
  constructor(
    message: string,
    public stepName: string,
    options?: ErrorOptions
  ) {
    super(message, options)
    this.name = this.constructor.name
  }
}

export class TransformAssertError extends TransformStepError {
  constructor(message: string, stepName: string, options?: ErrorOptions) {
    super(message, stepName, options)
    this.name = this.constructor.name
  }
}

export class TransformHeaderError
  extends TransformStepError
  implements ErrorWithReporter
{
  constructor(
    message: string,
    stepName: string,
    public header: ColumnHeader[],
    options?: ErrorOptions
  ) {
    super(message, stepName, options)
    this.name = this.constructor.name
  }

  report() {
    console.group(`[${this.stepName}] ${this.name}: ${this.message}`)
    console.table(
      this.header.map(h => ({
        'column': h.name,
        'internal index': h.index,
        'deleted': h.isDeleted
      }))
    )
    console.groupEnd()
  }
}

export class TransformColumnsNotFoundError extends TransformHeaderError {
  constructor(
    stepName: string,
    header: ColumnHeader[],
    public columns: string[],
    options?: ErrorOptions
  ) {
    const message =
      'Column(s) not found: ' + columns.map(c => `"${c}"`).join(', ')

    super(message, stepName, header, options)
    this.name = this.constructor.name
  }
}

function rowReport(this: TransformChunkError, rowCaption: string) {
  console.group(`[${this.stepName}] ${this.name}: ${this.message}`)
  console.info(`${rowCaption}:`)
  console.table(
    this.header.map(h => ({
      'column': h.name,
      'internal index': h.index,
      'deleted': h.isDeleted,
      'value': this.row[h.index]
    }))
  )
  console.info(`${this.chunk.length} rows in chunk`)
  console.groupEnd()
}

export class TransformChunkError extends TransformHeaderError {
  public row: TableRow

  constructor(
    message: string,
    stepName: string,
    header: ColumnHeader[],
    public chunk: TableRow[],
    options?: ErrorOptions
  ) {
    super(message, stepName, header, options)
    this.name = this.constructor.name
    this.row = chunk[0] ?? []
  }

  override report(): void {
    rowReport.call(this, 'First row sample')
  }

  getRowRecord() {
    return getRowRecord(this.header, this.chunk[0])
  }
}

export class TransformRowError extends TransformChunkError {
  column: string | undefined
  rowNum?: number | undefined = undefined

  constructor(
    message: string,
    stepName: string,
    header: ColumnHeader[],
    chunk: TableRow[],
    public rowIndex: number,
    public columnIndex: number | null,
    options?: ErrorOptions & { rowNum?: number | undefined }
  ) {
    super(message, stepName, header, chunk, options)
    this.name = this.constructor.name
    this.row = [...chunk[rowIndex]!]
    this.column = columnIndex != null ? header[columnIndex]?.name : undefined
    this.rowNum = options?.rowNum
  }

  override report(caption?: string): void {
    console.group(`[${this.stepName}] ${this.name}: ${this.message}`)

    if (caption != null) console.info(caption)

    if (this.rowNum != null) {
      console.info(`Row number: ${this.rowNum}`)
    }

    console.table(
      this.header.map(h => ({
        'column': h.name,
        'row index': h.index,
        'deleted': h.isDeleted,
        'value': this.row[h.index],
        ...(h.index === this.columnIndex ? { error: this.message } : {})
      })),
      [
        'column',
        'row index',
        'deleted',
        'value',
        ...(this.columnIndex != null ? ['error'] : [])
      ]
    )

    console.info(
      `${this.rowIndex + 1} row of ${this.chunk.length} rows in chunk`
    )

    console.groupEnd()
  }

  override getRowRecord(): Record<string, unknown> {
    return getRowRecord(this.header, this.chunk[this.rowIndex])
  }
}

export class TransformRowExpressionError extends TransformRowError {
  constructor(
    message: string,
    stepName: string,
    header: ColumnHeader[],
    chunk: TableRow[],
    rowIndex: number,
    columnIndex: number | null,
    public expression: string,
    options?: ErrorOptions & { rowNum?: number | undefined }
  ) {
    super(message, stepName, header, chunk, rowIndex, columnIndex, options)
    this.name = this.constructor.name
  }

  override report(): void {
    super.report(`Expression: ${this.expression}`)
  }
}
