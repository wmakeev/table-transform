export function filter(arr: unknown, fn: unknown) {
  if (typeof fn !== 'function') {
    throw new Error('Second argument in filter should be function')
  }

  if (!Array.isArray(arr)) return []

  return arr.filter(val => fn(val))
}
