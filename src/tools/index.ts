import { ColumnHeader, TableRow } from '../types.js'

export * from './AsyncChannel/index.js'
export * from './header/index.js'
export * from './stream/index.js'

export const UNICODE_SPACES_REGEX =
  /[\s\u00A0\u180E\u2000-\u200B\u202F\u205F\u3000\uFEFF]/gm

export function getPromiseWithResolvers<T = unknown>() {
  let resolve: (value: T) => void
  let reject: (reason?: any) => void

  const promise = new Promise<T>((resolve_, reject_) => {
    resolve = resolve_
    reject = reject_
  })

  return { promise, resolve: resolve!, reject: reject! }
}

export function getColumnIndexesByName(header: ColumnHeader[]) {
  const columnIndexesByName = header
    .filter(h => !h.isDeleted)
    .reduce((res, h) => {
      const exist = res.get(h.name)

      if (exist) {
        exist.push(h.index)
      } else {
        res.set(h.name, [h.index])
      }

      return res
    }, new Map<string, number[]>())

  return columnIndexesByName
}

export function getRowRecord(
  header: ColumnHeader[],
  row: TableRow | undefined
): Record<string, unknown | unknown[]> {
  if (row == null) return {}

  const entries = [...getColumnIndexesByName(header).entries()].map(
    ([key, val]) => {
      if (val.length === 1) {
        return [key, row[val[0]!]]
      } else {
        return [key, val.map(i => row[i])]
      }
    }
  )

  return Object.fromEntries(entries)
}

export function forceArrayLength(arr: unknown[], length: number): void {
  if (arr.length === length) {
    return
  } else if (arr.length > length) {
    arr.splice(length)
  } else {
    arr.push(...Array(length - arr.length).fill(null))
  }
}
