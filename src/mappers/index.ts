import { decodeHtml } from './decodeHtml.js'

const mapperByCode = {
  DECODE_HTML: decodeHtml
} as const

export interface MapperInfo {
  description: string
  mapper: (val: unknown) => unknown
}

export type MapperCode = keyof typeof mapperByCode

export const mappers: Record<MapperCode, MapperInfo> = {
  DECODE_HTML: {
    description: 'decodeHtml',
    mapper: mapperByCode.DECODE_HTML
  }
}
