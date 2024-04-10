import { UNICODE_SPACES_REGEX } from '../index.js'
import * as arr from './arr/index.js'
import * as barcode from './barcode/index.js'
import { curryWrapper } from './curryWrapper.js'
import { neq } from './neq.js'
import * as num from './num/index.js'
import * as str from './str/index.js'
import { vlookup } from './vlookup.js'

const functionsByName: Record<string, (...args: any[]) => any> = {
  'TypeOf': (val: unknown) => {
    return Object.prototype.toString
      .call(val)
      .slice(1, -1)
      .split(' ')[1]
      ?.toLowerCase()
  },

  'Table:VLookup': vlookup,

  'NotEqual': neq,

  'Arr:Filter': arr.filter,

  'Str:Trim': (val: unknown) => {
    if (typeof val === 'string') return val.trim()
    else return val
  },

  'Str:ToLowerCase': (val: unknown) => {
    if (typeof val === 'string') return val.toLowerCase()
    else return val
  },

  'Str:StartsWith': (str: unknown, value: unknown) => {
    // TODO Подумать
    return String(str).startsWith(String(value))
  },

  'Str:ReplaceAll': (
    str: unknown,
    searchValue: unknown,
    replaceValue: unknown
  ) => {
    return String(str).replaceAll(String(searchValue), String(replaceValue))
  },

  'Str:Split': (str: unknown, separator: unknown) => {
    return String(str).split(String(separator))
  },

  'Str:ExtractNums': str.extractNums,

  'Num:ToFixed': num.toFixed,

  'Num:TryParseFloat': (num: unknown, defaultValue = '') => {
    if (typeof num === 'number') {
      return num
    } else if (typeof num === 'string') {
      const parsed = Number.parseFloat(
        num.replaceAll(',', '.').replaceAll(UNICODE_SPACES_REGEX, '')
      )
      if (Number.isNaN(parsed)) return defaultValue
      return parsed
    } else {
      return defaultValue
    }
  },

  'Num:TryParseInt': num.tryParseInt,
  'Num:Max': num.max,
  'Num:Min': num.min,

  'Date:ToJson': (date: unknown) => {
    if (date instanceof Date) {
      return date.toJSON()
    } else if (typeof date === 'string') {
      return new Date(date).toJSON()
    } else {
      return ''
    }
  },

  'Array:At': (arr: unknown[], index: number) => {
    return arr.at(index)
  },

  // TODO Переименовать Str:getLeftPad(...)
  'Tools:GetTabSize': (str: unknown, tab = ' ') => {
    if (typeof str !== 'string') return 0

    let spaces = 0

    let _str = str

    while (_str.startsWith(tab)) {
      spaces++
      _str = _str.slice(tab.length)
    }

    return spaces
  },

  // TODO Переименовать Str:removeExtraSpaces(...)
  'Tools:RemoveExtraSpaces': (str: unknown) => {
    if (typeof str !== 'string') return str
    return str.replaceAll(/\s{2,}/g, ' ')
  },

  'Barcode:IsGTIN': barcode.isGTIN
}

const wrappedFunctionsByName = Object.fromEntries(
  [...Object.entries(functionsByName)].map(([name, fn]) => {
    return [name, curryWrapper(fn)]
  })
)

export const functions = wrappedFunctionsByName
