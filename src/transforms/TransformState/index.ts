import { DataRow, TableHeaderMeta } from '../../index.js'
import { TransformExpressionParams } from '../index.js'
import { getRowProxyHandler } from './getRowProxyHandler.js'
import {
  TransformExpressionContext,
  getTransformExpression
} from './getTransformExpression.js'

export class TransformState {
  public rowNum = 0
  public arrColIndex = 0
  public curRow!: DataRow

  /**
   * Indexes of source columns with transfomed column header name
   */
  public fieldColsIndexes

  /**
   * Indexes of source columns by current header cloumns names
   */
  public fieldIndexesByName

  private rowProxy
  private transformExpression

  constructor(
    transformParams: TransformExpressionParams,
    header: TableHeaderMeta,
    context?: TransformExpressionContext
  ) {
    const { columnName } = transformParams

    this.fieldColsIndexes = header.flatMap(h => {
      if (h.name === columnName) return h.srcIndex
      else return []
    })

    if (this.fieldColsIndexes.length === 0) {
      throw new Error(`Collumn "${columnName}" not found`)
    }

    this.fieldIndexesByName = header.reduce((res, h) => {
      const indexes = res.get(h.name)

      if (indexes) {
        indexes.push(h.srcIndex)
      } else {
        res.set(h.name, [h.srcIndex])
      }

      return res
    }, new Map<string, number[]>())

    this.rowProxy = new Proxy(this, getRowProxyHandler(header, this))

    this.transformExpression = getTransformExpression(
      transformParams,
      this,
      context
    )
  }

  nextRow(row: DataRow) {
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
