export function extractNums(val: unknown) {
  if (typeof val === 'string') {
    // TODO Кстати, а нужно ли кешировать такие регулярки?
    return val.replaceAll(/\D/gm, '')
  }

  return val
}
