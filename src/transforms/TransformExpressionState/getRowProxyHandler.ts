import { TransformColumnsNotFoundError } from '../../errors/index.js'
import { TableHeader } from '../../index.js'
import { TransformExpressionState } from './index.js'

export const getRowProxyHandler = (
  header: TableHeader,
  transformState: TransformExpressionState
) => {
  /** Headers list */
  const fields = header.filter(h => !h.isDeleted).map(h => h.name)

  /** Uniq columns names */
  const fieldsSet = new Set(fields)

  /** Uniq columns names */
  const uniqFields = [...fieldsSet.values()]

  const rowProxyHandler: ProxyHandler<TransformExpressionState> = {
    has(_, key) {
      if (typeof key !== 'string') return false
      return fieldsSet.has(key)
    },

    ownKeys() {
      return uniqFields
    },

    get(target, prop) {
      if (prop === Symbol.toStringTag) return 'Object'

      const indexes = transformState.fieldIndexesByName.get(prop as string)

      if (indexes == null) {
        throw new TransformColumnsNotFoundError(transformState.name, header, [
          String(prop)
        ])
      }

      if (indexes.length === 1) {
        return target.curRow[indexes[0]!]
      } else {
        const values = []

        for (const index of indexes) {
          values.push(target.curRow[index])
        }

        return values
      }
    },

    getOwnPropertyDescriptor(target, prop) {
      if (fieldsSet.has(prop as string) == null) return undefined

      return {
        configurable: true,
        enumerable: true,
        value: rowProxyHandler.get!(target, prop, null)
      }
    }
  }

  return rowProxyHandler
}
