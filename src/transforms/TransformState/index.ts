import { ColumnHeader, TableRow } from '../../index.js'
import { TransformExpressionParams } from '../index.js'
import { getRowProxyHandler } from './getRowProxyHandler.js'
import {
  TransformExpressionContext,
  getTransformExpression
} from './getTransformExpression.js'

export type { TransformExpressionContext } from './getTransformExpression.js'

export class TransformState {
  public rowNum = 0
  public arrColIndex = 0
  public curRow!: TableRow

  /**
   * Column name specified for this transform
   */
  public columnName: string | null

  /**
   * Indexes of source columns with transfomed column header name
   */
  public fieldColsIndexes = [] as number[]

  /**
   * Indexes of source columns by current header cloumns names
   */
  public fieldIndexesByName = new Map<string, number[]>()

  private rowProxy
  private transformExpression

  constructor(
    transformParams: TransformExpressionParams,
    header: ColumnHeader[],
    context?: TransformExpressionContext
  ) {
    const { columnName } = transformParams

    this.columnName = columnName ?? null

    if (columnName != null) {
      this.fieldColsIndexes = header.flatMap(h => {
        if (!h.isDeleted && h.name === columnName) return h.index
        else return []
      })

      if (this.fieldColsIndexes.length === 0) {
        throw new Error(`Column "${columnName}" not found`)
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

    this.transformExpression = getTransformExpression(
      transformParams,
      this,
      context
    )
  }

  nextRow(row: TableRow) {
    this.curRow = row
    this.rowNum++
  }

  setArrColIndex(index: number) {
    this.arrColIndex = index
  }

  evaluateExpression() {
    const result = this.transformExpression(this.rowProxy)

    if (result instanceof Error) {
      const colIndex = this.fieldColsIndexes[this.arrColIndex]

      const dump = [...this.fieldIndexesByName.entries()]
        .flatMap(([field, indexes]) => {
          return indexes.map(
            i =>
              (i === colIndex ? '*' : ' ') +
              `  [${i}] "${field}": ${JSON.stringify(this.curRow[i])}` +
              (i === colIndex ? ` - ${result.message}` : '')
          )
        })
        .join('\n')

      console.log(`rowNum: ${this.rowNum}, colIndex: ${colIndex}\n\n${dump}`)

      throw result
    }

    return result
  }
}
