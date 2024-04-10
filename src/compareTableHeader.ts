import { TableHeaderMeta } from './types.js'

export const compareTableHeader = (a: TableHeaderMeta, b: TableHeaderMeta) => {
  if (a === b) return true
  if (a.length !== b.length) return false

  for (let i = 0; i <= a.length; i++) {
    const aHead = a[i]
    const bHead = b[i]

    const isPass = aHead?.srcIndex === bHead?.srcIndex

    if (!isPass) return false
  }

  return true
}
