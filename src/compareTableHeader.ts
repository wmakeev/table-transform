import { ColumnHeader } from './types.js'

export const compareTableHeader = (a: ColumnHeader[], b: ColumnHeader[]) => {
  if (a === b) return true
  if (a.length !== b.length) return false

  for (let i = 0; i <= a.length; i++) {
    const aHead = a[i]
    const bHead = b[i]

    const isPass =
      aHead?.index === bHead?.index && aHead?.isDeleted === bHead?.isDeleted

    if (!isPass) return false
  }

  return true
}
