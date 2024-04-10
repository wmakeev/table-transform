import { UNICODE_SPACES_REGEX } from '../../index.js'

export * from './tryParseInt.js'

export const min = (vals: unknown[]) => {
  if (!Array.isArray(vals)) {
    throw new Error('min function expected array argument')
  }
  const nums = vals.filter(n => Number.isFinite(n)) as number[]
  return Math.min(...nums)
}

export const max = (vals: unknown[]) => {
  if (!Array.isArray(vals)) {
    throw new Error('max function expected array argument')
  }
  const nums = vals.filter(n => Number.isFinite(n)) as number[]
  return Math.max(...nums)
}

// TODO Убрать defaultValue, использовать tryParse..
export const toFixed = (
  num: unknown,
  fractionDigits = 0,
  defaultValue: unknown = 0
) => {
  if (typeof num === 'number') {
    return num.toFixed(fractionDigits)
  } else if (typeof num === 'string') {
    const parsed = Number.parseFloat(
      num.replaceAll(',', '.').replaceAll(UNICODE_SPACES_REGEX, '')
    )
    if (Number.isNaN(parsed)) return defaultValue
    return parsed.toFixed(fractionDigits)
  } else {
    return defaultValue
  }
}

// TODO Num:round(num, 2)
