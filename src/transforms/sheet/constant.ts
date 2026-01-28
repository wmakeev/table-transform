import { TableChunksTransformer } from '../../index.js'
import { TransformBaseParams } from '../index.js'
import { NonEmptyTestValueOperations, sheetCell } from './cell.js'

// TODO Нужно как-то переопределять наименование трансформации
// const TRANSFORM_NAME = 'Sheet:Column'

export interface BaseSheetConstantParams extends TransformBaseParams {
  /**
   * The range to search for a constant cell.
   *
   * Excel style range like: `A1`, `A2:B10`
   */
  range: string

  /**
   * Column to place constant value
   */
  targetColumn: string

  /**
   * Array index, if `targetColumn` is array column
   */
  targetArrColumnIndex?: number

  /**
   * The offset to be shifted to target cell after the cell with `cellName`
   * is found in `range`.
   *
   * Excel RC style offset like: `R[1]`, `C[2]`, `R[1]C[1]`
   */
  offset?: string | undefined

  /**
   * If `true` not error was thrown if column not found
   */
  isOptional?: boolean
}

export type SheetConstantParams =
  | BaseSheetConstantParams
  | (BaseSheetConstantParams & {
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
export const constant = (
  params: SheetConstantParams
): TableChunksTransformer => {
  return sheetCell({
    type: 'CONSTANT',
    ...params
  })
}
