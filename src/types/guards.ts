export function isObjectGuard(obj: unknown): obj is object {
  return Object.prototype.toString.call(obj) === '[object Object]'
}
