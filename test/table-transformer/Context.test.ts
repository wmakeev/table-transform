import test from 'node:test'
import { Context } from '../../src/index.js'
import assert from 'node:assert/strict'

test('Context', () => {
  const scopeSym1 = Symbol('scope1')

  const parentContext = new Context()

  parentContext.set(scopeSym1, 'foo', 'bar11')
  parentContext.set(scopeSym1, 'num', 42)

  const childContext = new Context(parentContext)

  childContext.set(scopeSym1, 'foo', 'bar12')

  assert.equal(childContext.get(scopeSym1, 'foo'), 'bar12')
  assert.equal(childContext.get(scopeSym1, 'num'), 42)

  assert.equal(childContext.set(scopeSym1, 'foo', 'baz11'), false)
  assert.equal(childContext.get(scopeSym1, 'foo'), 'baz11')

  assert.equal(childContext.set(scopeSym1, 'foo2', 'baz12'), true)
  assert.equal(childContext.get(scopeSym1, 'foo2'), 'baz12')

  assert.equal(parentContext.get(scopeSym1, 'foo'), 'bar11')

  const scopeSym2 = Symbol('scope2')

  assert.equal(childContext.get(scopeSym2, 'foo'), undefined)
  assert.equal(childContext.set(scopeSym2, 'foo', 'baz21'), true)
  assert.equal(childContext.get(scopeSym2, 'foo'), 'baz21')
})
