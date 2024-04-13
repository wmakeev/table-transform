const tablesLookupCache = new WeakMap<unknown[][], Map<unknown, unknown>>()

const createTableLookupCache = (
  table: unknown[][],
  colNum: number | string
) => {
  let colNum_: number

  if (typeof colNum === 'string') {
    const headIndex = table[0]?.indexOf(colNum)

    if (headIndex === undefined || headIndex === -1) {
      throw new Error(`vlookup: header not found - ${colNum}`)
    }

    colNum_ = headIndex + 1
  } else {
    colNum_ = colNum
  }

  if (table[0] && table[0].length < colNum_) {
    throw new Error(`vlookup: index ${colNum_} is out of range`)
  }

  return table.reduce((res, row) => {
    if (res.has(row[0])) {
      return res
    }

    res.set(row[0], row[colNum_ - 1])

    return res
  }, new Map<unknown, unknown>())
}

export function vlookup(
  searchKey: unknown,
  table: unknown[][],
  index: number | string
) {
  if (table == null) {
    throw new Error('vlookup: table argument not defined')
  }

  if (!Array.isArray(table)) {
    throw new Error('vlookup: table argument should to be array')
  }

  let tableLookupCache = tablesLookupCache.get(table)

  if (tableLookupCache === undefined) {
    tableLookupCache = createTableLookupCache(table, index)
    tablesLookupCache.set(table, tableLookupCache)
  }

  return tableLookupCache.get(searchKey)
}
