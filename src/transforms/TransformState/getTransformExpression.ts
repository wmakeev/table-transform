import { compileExpression } from '@wmakeev/filtrex'
import { TransformExpressionParams, TransformState } from '../index.js'
import {
  TransformColumnsNotFoundError,
  TransformError,
  TransformSymbolNotFoundError
} from '../../errors/index.js'

function optionalPropertyAccessor(
  name: string,
  get: (name: string) => any,
  obj: object | undefined,
  type: 'single-quoted' | 'unescaped',
  isNested: boolean
) {
  if (obj === null || obj === undefined) return obj

  // Force quoted data field access
  if (isNested === false && type !== 'single-quoted') {
    try {
      get(name)
    } catch (err) {
      if (err instanceof TransformColumnsNotFoundError) {
        throw new TransformSymbolNotFoundError(err.stepName, err.header, name)
      }
    }

    throw new TransformError(
      `Field "${name}" access with unquoted name - try to use quoted notation "'${name}'"`
    )
  }

  if (isNested) {
    try {
      return get(name)
    } catch (err) {
      if (err instanceof Error && err.name === 'ReferenceError') {
        return undefined
      }
      throw err
    }
  } else {
    return get(name)
  }
}

export interface TransformExpressionContext {
  symbols?:
    | {
        [T: string]: any
      }
    | undefined
}

export const getTransformExpression = (
  params: TransformExpressionParams,
  transformState: TransformState,
  context?: TransformExpressionContext
) => {
  const transformExpression = compileExpression(params.expression, {
    customProp: optionalPropertyAccessor,

    symbols: {
      ...(context?.symbols ?? {}),

      value: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const actualColumnName = columnName ?? params.column

        if (actualColumnName == null) return null

        const index =
          transformState.fieldIndexesByName.get(actualColumnName)?.[
            transformState.arrColIndex
          ]

        return index === undefined ? '' : (transformState.curRow[index] ?? '')
      },

      values: (columnName: unknown) => {
        if (columnName != null && typeof columnName !== 'string') {
          throw new Error('values() argument expected to be string')
        }

        const actualColumnName = columnName ?? params.column

        if (actualColumnName == null) return []

        const indexes = transformState.fieldIndexesByName.get(actualColumnName)

        if (indexes === undefined) return ''

        return indexes.map(i => transformState.curRow[i] ?? '')
      },

      row: () => transformState.rowNum,

      column: () => transformState.column,

      // TODO Name is not obvious
      arrayIndex: () => transformState.arrColIndex
    }
  })

  return transformExpression
}
