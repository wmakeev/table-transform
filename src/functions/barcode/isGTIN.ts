/**
 * Check if code is GTIN (EAN8, EAN12, EAN13, EAN14)
 *
 * @link https://github.com/hampus-nilsson/gs1-checkdigit/blob/main/checkdigit.js
 */
export function isGTIN(val: unknown): boolean {
  const input = String(val)

  if (![8, 12, 13, 14].includes(input.length)) return false

  const digs = input.split('').reverse()

  let total = 0

  for (let i = 1; i < digs.length; i++) {
    const num = parseInt(digs[i]!)

    if (Number.isNaN(num)) return false

    if (i % 2 === 0) {
      total += num
    } else {
      total += num * 3
    }
  }

  const check = Math.ceil(total / 10) * 10 - total

  return parseInt(digs[0]!) === check
}
