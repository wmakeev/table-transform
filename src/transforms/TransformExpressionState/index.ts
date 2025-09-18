import assert from 'node:assert'
import {
  TransformHeaderError,
  TransformStepParameterError
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
  public column: string | null

  /**
   * Indexes of source columns with transformed column header name
   */
  public fieldColsIndexes = [] as number[]

  /**
   * Indexes of source columns by current header columns names
   */
  public fieldIndexesByName = new Map<string, number[]>()

  private rowProxy
  private transformExpressionFunc

  constructor(
    public name: string,
    transformParams: TransformExpressionParams,
    header: TableHeader,
    context: Context
  ) {
    const { column } = transformParams

    this.column = column ?? null

    if (column != null) {
      this.fieldColsIndexes = header.flatMap(h => {
        if (!h.isDeleted && h.name === column) return h.index
        else return []
      })

      if (this.fieldColsIndexes.length === 0) {
        throw new TransformHeaderError(
          `Column "${column}" not found`,
          name,
          header
        )
      }
    }

    this.fieldIndexesByName = header
      .filter(h => !h.isDeleted)
      .reduce((res, h) => {
        const indexes = res.get(h.name)

        if (indexes) {
          indexes.push(h.index)
        } else {
          res.set(h.name, [h.index])
        }

        return res
      }, this.fieldIndexesByName)

    this.rowProxy = new Proxy(this, getRowProxyHandler(header, this))

    try {
      this.transformExpressionFunc = compileTransformExpression(
        this,
        transformParams,
        context
      )
    } catch (err) {
      assert.ok(err instanceof Error)
      throw new TransformStepParameterError(
        err.message,
        this.name,
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

  setArrColIndex(index: number) {
    this.arrColIndex = index
  }

  evaluateExpression(): unknown | Error {
    const result = this.transformExpressionFunc(this.rowProxy)
    return result
  }
}
