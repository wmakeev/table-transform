import assert from 'node:assert'
import {
  TransformStepColumnIndexOverBoundsError,
  TransformStepColumnNotFoundError,
  TransformStepError,
  TransformStepHeaderError,
  TransformStepParameterError,
  TransformStepTypeError
} from '../../errors/index.js'
import { Context, TableHeader, TableRow } from '../../index.js'
import { TransformExpressionParams } from '../index.js'
import { compileTransformExpression } from './compileTransformExpression.js'
import { getRowProxyHandler } from './getRowProxyHandler.js'
export type { ExpressionContext as TransformExpressionContext } from '../../types/ExpressionContext.js'

export class TransformExpressionState {
  public rowNum = 0
  public arrColIndex = 0
  public curRow!: TableRow

  /**
   * Column name specified for this transform
   */
  public curColName: string | null

  /**
   * Current transform column index
   */
  public curColDefaultSrcIndex: number | null = null

  /**
   * Current row columns names
   */
  public rowColumns: string[] = []

  /**
   * Indexes of source columns with transformed column header name
   */
  public curColSrcIndexes: number[] | null = null

  /**
   * Indexes of source columns by current header columns names
   */
  public srcIndexesByColName = new Map<string, number[]>()

  public actualTableHeader = [] as TableHeader

  private rowProxy
  private transformExpressionFunc

  constructor(
    public stepName: string,
    transformParams: TransformExpressionParams,
    tableHeader: TableHeader,
    context: Context
  ) {
    const { column } = transformParams

    this.curColName = column ?? null

    this.actualTableHeader = tableHeader.filter(h => !h.isDeleted)

    this.rowColumns = this.actualTableHeader.map(h => h.name)

    this.srcIndexesByColName = this.actualTableHeader.reduce((res, h) => {
      const indexes = res.get(h.name)

      if (indexes) {
        indexes.push(h.index)
      } else {
        res.set(h.name, [h.index])
      }

      return res
    }, this.srcIndexesByColName)

    if (this.curColName != null) {
      this.curColSrcIndexes = tableHeader.flatMap(h => {
        if (!h.isDeleted && h.name === this.curColName) return h.index
        else return []
      })

      if (this.curColSrcIndexes.length === 0) {
        throw new TransformStepHeaderError(
          `Column "${this.curColName}" not found`,
          stepName,
          tableHeader
        )
      }

      this.curColDefaultSrcIndex =
        this.srcIndexesByColName.get(this.curColName)![0] ?? null
    }

    this.rowProxy = new Proxy(this, getRowProxyHandler(tableHeader, this))

    try {
      this.transformExpressionFunc = compileTransformExpression(
        this,
        transformParams,
        context,
        {
          createDefaultColumNotSetError() {
            return new TransformStepError(
              "Can't get the default column value because it is not set.",
              stepName
            )
          },
          createColumnNotFoundError(column, index) {
            return new TransformStepColumnNotFoundError(
              stepName,
              tableHeader,
              column,
              index
            )
          },
          createColumnIndexOverBoundsError(columnIndex) {
            return new TransformStepColumnIndexOverBoundsError(
              stepName,
              tableHeader,
              columnIndex
            )
          },
          createIncorrectColumnNameTypeError(message) {
            return new TransformStepTypeError(message, stepName)
          }
        }
      )
    } catch (err) {
      assert.ok(err instanceof Error)
      throw new TransformStepParameterError(
        err.message,
        this.stepName,
        'expression',
        transformParams.expression,
        { cause: err }
      )
    }
  }

  nextRow(row: TableRow) {
    this.curRow = row
    this.rowNum++
  }

  // setArrColIndex(index: number) {
  //   this.arrColIndex = index
  // }

  evaluateExpression(): unknown | Error {
    const result = this.transformExpressionFunc(this.rowProxy)
    return result
  }
}
