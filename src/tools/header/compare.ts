import { ColumnHeader } from '../../index.js'

export const compareTableRawHeader = (a: ColumnHeader[], b: ColumnHeader[]) => {
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

export const compareTableActualHeader = (
  a: ColumnHeader[],
  b: ColumnHeader[]
) => {
  const aHeader = a.filter(h => !h.isDeleted).map(h => h.name)
  const bHeader = b.filter(h => !h.isDeleted).map(h => h.name)

  if (aHeader.length !== bHeader.length) return false

  for (let i = 0; i <= aHeader.length; i++) {
    if (aHeader[i] !== bHeader[i]) return false
  }

  return true
}
