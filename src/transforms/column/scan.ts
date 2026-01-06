import assert from 'node:assert'
import { TransformStepRowExpressionError } from '../../errors/index.js'
import { Context, TableChunksTransformer } from '../../index.js'
import {
  ColumnTransformExpressionParams,
  TransformExpressionContext,
  TransformExpressionState
} from '../index.js'

const TRANSFORM_NAME = 'Column:Scan'

class ScanContext extends Context {
  constructor(
    parent: Context,
    public getPrev: () => unknown
  ) {
    super(parent)
  }

  override getExpressionContext(): TransformExpressionContext {
    return {
      ...super.getExpressionContext(),
      prev: this.getPrev
    }
  }
}

export interface ColumnScanExpressionParams
  extends ColumnTransformExpressionParams {
  seed?: unknown
}

export const scan = (
  params: ColumnScanExpressionParams
): TableChunksTransformer => {
  assert.ok(
    typeof params.column === 'string',
    'scan column parameter expected to be string'
  )

  return source => {
    async function* getTransformedSourceGenerator() {
      const { columnIndex = null, seed } = params

      const hasSeed = seed !== undefined

      let seedInitiated = hasSeed
      let prevResult = params.seed

      const getPrev = () => prevResult

      const scanContext = new ScanContext(source.getContext(), getPrev)

      const srcHeader = source.getTableHeader()

      const transformState: TransformExpressionState =
        new TransformExpressionState(
          TRANSFORM_NAME,
          params,
          srcHeader,
          scanContext
        )

      const srcIndexes = transformState.curColSrcIndexes!

      for await (const chunk of source) {
        for (const ent of chunk.entries()) {
          const rowIndex = ent[0]
          const row = ent[1]

          transformState.nextRow(row)

          for (const srcIndexEnt of srcIndexes.entries()) {
            const arrColIndex = srcIndexEnt[0]
            const headerColIndex = srcIndexEnt[1]

            if (columnIndex !== null && arrColIndex !== columnIndex) {
              continue
            }

            if (seedInitiated === false) {
              prevResult = row[headerColIndex]
              seedInitiated = true
              continue
            }

            transformState.arrColIndex = arrColIndex

            const result = transformState.evaluateExpression()

            if (result instanceof Error) {
              throw new TransformStepRowExpressionError(
                result.message,
                TRANSFORM_NAME,
                srcHeader,
                chunk,
                rowIndex,
                headerColIndex,
                params.expression,
                { cause: result, rowNum: transformState.rowNum }
              )
            }

            row[headerColIndex] = result
            prevResult = result
          }
        }

        yield chunk
      }
    }

    return {
      ...source,
      getTableHeader: () => source.getTableHeader(),
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
