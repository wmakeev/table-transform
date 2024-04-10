import decode from 'decode-html'

export const decodeHtml = (val: unknown) => {
  if (typeof val === 'string') {
    return decode(val)
  }

  return val
}
