import { UNICODE_SPACES_REGEX } from '../../index.js'

export function tryParseInt(num: unknown, defaultValue = '') {
  if (typeof num === 'number') {
    return num
  } else if (typeof num === 'string') {
    const parsed = Number.parseInt(num.replaceAll(UNICODE_SPACES_REGEX, ''))
    if (Number.isNaN(parsed)) return defaultValue
    return parsed
  } else {
    return defaultValue
  }
}
