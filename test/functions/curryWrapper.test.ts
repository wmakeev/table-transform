import assert from 'node:assert/strict'
import test from 'node:test'
import {
  CURRY_PLACEHOLDER as _,
  curryWrapper
} from '../../src/functions/curryWrapper.js'

test('curryWrapper (noop)', () => {
  const fn1 = () => {
    return 42
  }

  const fn1Wrapped = curryWrapper(fn1)

  assert.equal(fn1Wrapped(), 42)
  assert.equal(fn1Wrapped(123), 42)

  const fn1WrappedCurry1 = fn1Wrapped(_)

  assert.equal(typeof fn1WrappedCurry1, 'function')
  assert.equal(fn1WrappedCurry1(123), 42)

  const fn1WrappedCurry2 = fn1Wrapped(_, 89)

  assert.equal(typeof fn1WrappedCurry2, 'function')
  assert.equal(fn1WrappedCurry2(123), 42)
})

test('curryWrapper (curry)', () => {
  const sub = (a: number, b: number) => {
    assert.equal(typeof a, 'number')
    assert.equal(typeof b, 'number')
    return a - b
  }

  const subWrapped = curryWrapper(sub)

  assert.equal(subWrapped(4, 2), 2)

  const sub1 = subWrapped(_, 1)

  assert.equal(typeof sub1, 'function')
  assert.equal(sub1(123), 122)

  const subFrom10 = subWrapped(10, _)

  assert.equal(typeof subFrom10, 'function')
  assert.equal(subFrom10(3), 7)
})
