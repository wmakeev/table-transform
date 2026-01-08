import { TableChunksTransformer } from '../../index.js'
import {
  EmptyTestValueOperations,
  NonEmptyTestValueOperations,
  sheetCell
} from './cell.js'

// TODO Нужно как-то переопределять наименование трансформации
// const TRANSFORM_NAME = 'Sheet:Assert'

export interface BaseSheetAssertParams {
  /**
   * The range to search for a cell to make assertion.
   *
   * Excel style range like: `A1`, `A2:B10`
   */
  range: string
}

export type SheetAssertParams =
  | (BaseSheetAssertParams & {
      testValue?: never
      testOperation: EmptyTestValueOperations
    })
  | (BaseSheetAssertParams & {
      /**
       * A cell in the `range` whose value corresponds to `testValue` will be
       * searched using the `testOperation` method for matching. By default
       * `testOperation` is `EQUAL`.
       */
      testValue: string | number

      /**
       * Method for matching `testValue` and seaching cell value.
       * Default - `EQUAL`.
       */
      testOperation?: NonEmptyTestValueOperations | undefined
    })

/**
 * Extract sheet column content
 */
export const assert = (params: SheetAssertParams): TableChunksTransformer => {
  return sheetCell(
    params.testValue != null
      ? {
          type: 'ASSERT',
          range: params.range,
          testValue: params.testValue,
          testOperation: params.testOperation
        }
      : {
          type: 'ASSERT',
          range: params.range,
          testValue: undefined,
          testOperation: params.testOperation
        }
  )
}
