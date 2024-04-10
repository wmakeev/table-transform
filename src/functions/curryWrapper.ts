export const CURRY_PLACEHOLDER = Symbol('CURRY_PLACEHOLDER')

export const curryWrapper =
  (fn: Function) =>
  (...args: any[]) => {
    const phIndex = args.indexOf(CURRY_PLACEHOLDER)

    if (phIndex === -1) return fn(...args)

    return (phArg: any) => {
      args[phIndex] = phArg
      return fn(...args)
    }
  }
