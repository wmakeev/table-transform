import assert from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as setTimeoutAsync } from 'node:timers/promises'
import { AsyncChannel } from '../../src/index.js'

test('AsyncChannel #1', async () => {
  const timeline: string[] = []

  const consumer = async (ch: AsyncChannel) => {
    timeline.push('consumer: started')

    for await (const it of ch) {
      timeline.push(`consumer: chan > ${it}`)
    }

    timeline.push('consumer: stoped.')
  }

  const producer = async (ch: AsyncChannel) => {
    timeline.push('producer: started')

    timeline.push('producer: 1 > chan ..')
    timeline.push(`producer: 1 > chan (${await ch.put(1)})`)

    timeline.push('producer: 2, 3, 4  > chan ..')
    timeline.push(
      `producer: 2, 3, 4  > chan (${await Promise.all([ch.put(2), ch.put(3), ch.put(4)])})`
    )

    timeline.push('producer: 5 > chan ..')
    timeline.push(`producer: 5 > chan (${await ch.put(5)})`)

    timeline.push('producer: close')
    chan.close()

    timeline.push('producer: stoped.')
  }

  const chan = new AsyncChannel()

  await Promise.all([consumer(chan), producer(chan)])

  assert.deepEqual(
    timeline,
    //
    [
      'consumer: started',
      'producer: started',
      'producer: 1 > chan ..',
      'consumer: chan > 1',
      'producer: 1 > chan (true)',
      'producer: 2, 3, 4  > chan ..',
      'consumer: chan > 2',
      'consumer: chan > 3',
      'consumer: chan > 4',
      'producer: 2, 3, 4  > chan (true,true,true)',
      'producer: 5 > chan ..',
      'consumer: chan > 5',
      'producer: 5 > chan (true)',
      'producer: close',
      'producer: stoped.',
      'consumer: stoped.'
    ]
  )
})

test('AsyncChannel #2', async () => {
  const timeline: string[] = []

  const consumer = async (ch: AsyncChannel) => {
    for await (const it of ch) {
      timeline.push(`consumer: ${it}`)
      if (it === 3) break
    }

    timeline.push(`consumer: close`)
    ch.close()
  }

  const producer = async (ch: AsyncChannel) => {
    const results = []

    timeline.push(`producer: 1`)
    results.push(await ch.put(1))

    timeline.push(`producer: 2`)
    results.push(await ch.put(2))

    timeline.push(`producer: 3`)
    results.push(await ch.put(3))

    timeline.push(`producer: 4`)
    results.push(await ch.put(4))

    assert.deepEqual(results, [true, true, true, false])
  }

  const chan = new AsyncChannel()

  await Promise.all([consumer(chan), producer(chan)])

  assert.deepEqual(
    timeline,
    //
    [
      'producer: 1',
      'consumer: 1',
      'producer: 2',
      'consumer: 2',
      'producer: 3',
      'consumer: 3',
      'consumer: close',
      'producer: 4'
    ]
  )
})

test('AsyncChannel #3', async () => {
  const timeline: string[] = []

  const consumer = async (ch: AsyncChannel<number>) => {
    timeline.push('consumer: start')

    for await (const it of ch) {
      timeline.push(`consumer: > ${it}`)

      if (it > 3) {
        timeline.push(`consumer: timeout 10`)
        await setTimeoutAsync(10)
      }

      if (it === 7) {
        timeline.push(`consumer: break`)
        break
      }
    }

    timeline.push(`consumer: chan close`)
    chan.close()

    timeline.push('consumer: end.')
  }

  const producer = async (ch: AsyncChannel<number>) => {
    timeline.push('producer: start')

    timeline.push('producer: 1 >')
    const res1 = await ch.put(1)
    timeline.push(`producer: put(1) -> ${res1}`)

    timeline.push('producer: 2 >')
    const res2 = await ch.put(2)
    timeline.push(`producer: put(2) -> ${res2}`)

    timeline.push('producer: 3 >')
    const res3 = await ch.put(3)
    timeline.push(`producer: put(3) -> ${res3}`)

    timeline.push('producer: 4 >')
    const res4 = await ch.put(4)
    timeline.push(`producer: put(4) -> ${res4}`)

    timeline.push('producer: 5 >')
    const res5 = await ch.put(5)
    timeline.push(`producer: put(5) -> ${res5}`)

    timeline.push('producer: 6 >')
    const res6 = await ch.put(5)
    timeline.push(`producer: put(6) -> ${res6}`)

    timeline.push('producer: 7 >')
    const res7 = await ch.put(7)
    timeline.push(`producer: put(7) -> ${res7}`)

    timeline.push('producer: 98 >')
    const res98 = await ch.put(98)
    timeline.push(`producer: put(98) -> ${res98}`)

    timeline.push('producer: 99 >')
    const res99 = await chan.put(99)
    timeline.push(`producer: put(99) -> ${res99}`)

    timeline.push('producer end.')
  }

  const chan = new AsyncChannel<number>({ name: 'TestAsyncChannel' })

  await Promise.all([consumer(chan), producer(chan)])

  assert.deepEqual(
    timeline,
    //
    [
      'consumer: start',
      'producer: start',

      'producer: 1 >',
      'consumer: > 1',
      'producer: put(1) -> true',

      'producer: 2 >',
      'consumer: > 2',
      'producer: put(2) -> true',

      'producer: 3 >',
      'consumer: > 3',
      'producer: put(3) -> true',

      'producer: 4 >',
      'consumer: > 4',

      'consumer: timeout 10',

      'producer: put(4) -> true',

      'producer: 5 >',
      'producer: put(5) -> true',

      'producer: 6 >',

      'consumer: > 5',

      'consumer: timeout 10',

      'producer: put(6) -> true',

      'producer: 7 >',

      'consumer: > 5',

      'consumer: timeout 10',

      'producer: put(7) -> true',

      'producer: 98 >',

      'consumer: > 7',

      'consumer: timeout 10',

      'consumer: break',

      'consumer: chan close',

      'consumer: end.',

      'producer: put(98) -> false',

      'producer: 99 >',
      'producer: put(99) -> false',

      'producer end.'
    ]
  )
})

test('AsyncChannel #3', async () => {
  const timeline: string[] = []

  const consumer = async (ch: AsyncChannel<number>) => {
    timeline.push('consumer: start')

    for await (const it of ch) {
      timeline.push(`consumer: > ${it}`)

      if (it > 3) {
        timeline.push(`consumer: timeout 10`)
        await setTimeoutAsync(10)
      }
    }

    timeline.push('consumer: end.')
  }

  const producer = async (ch: AsyncChannel<number>) => {
    timeline.push('producer: start')

    timeline.push('producer: 1 >')
    const res1 = await ch.put(1)
    timeline.push(`producer: put(1) -> ${res1}`)

    timeline.push('producer: 2 >')
    const res2 = await ch.put(2)
    timeline.push(`producer: put(2) -> ${res2}`)

    timeline.push('producer: 3 >')
    const res3 = await ch.put(3)
    timeline.push(`producer: put(3) -> ${res3}`)

    timeline.push('producer: 4 >')
    const res4 = await ch.put(4)
    timeline.push(`producer: put(4) -> ${res4}`)

    timeline.push('producer: 5 >')
    ch.put(5).then(res5 => {
      timeline.push(`producer: put(5) -> ${res5}`)
    })

    timeline.push('producer: 6 >')
    ch.put(6).then(res6 => {
      timeline.push(`producer: put(6) -> ${res6}`)
    })

    timeline.push('producer: 7 >')
    ch.put(7).then(res7 => {
      timeline.push(`producer: put(7) -> ${res7}`)
    })

    timeline.push(`producer: wait flush`)
    await chan.flush()

    timeline.push(`producer: chan close`)
    chan.close()

    timeline.push('producer end.')
  }

  const chan = new AsyncChannel<number>({ name: 'TestAsyncChannel' })

  await Promise.all([consumer(chan), producer(chan)])

  assert.deepEqual(
    timeline,
    //
    [
      'consumer: start',
      'producer: start',

      'producer: 1 >',
      'consumer: > 1',
      'producer: put(1) -> true',

      'producer: 2 >',
      'consumer: > 2',
      'producer: put(2) -> true',

      'producer: 3 >',
      'consumer: > 3',
      'producer: put(3) -> true',

      'producer: 4 >',
      'consumer: > 4',
      'consumer: timeout 10',
      'producer: put(4) -> true',

      'producer: 5 >',
      'producer: 6 >',
      'producer: 7 >',

      'producer: wait flush',

      'producer: put(5) -> true',
      'consumer: > 5',

      'consumer: timeout 10',

      'producer: put(6) -> true',
      'consumer: > 6',

      'consumer: timeout 10',

      'producer: put(7) -> true',
      'consumer: > 7',

      'consumer: timeout 10',

      'producer: chan close',

      'producer end.',
      'consumer: end.'
    ]
  )
})

test('AsyncChannel (buffer) #1', async () => {
  const timeline: string[] = []

  const consumer = async (ch: AsyncChannel<number>) => {
    timeline.push('consumer: start')

    for await (const it of ch) {
      timeline.push(`consumer: > ${it}`)

      if (it > 3) {
        timeline.push(`consumer: timeout 10`)
        await setTimeoutAsync(10)
      }

      if (it === 7) {
        timeline.push(`consumer: break`)
        break
      }
    }

    timeline.push(`consumer: chan close`)
    chan.close()

    timeline.push('consumer: end.')
  }

  const producer = async (ch: AsyncChannel<number>) => {
    timeline.push('producer: start')

    timeline.push('producer: 1 >')
    const res1 = await ch.put(1)
    timeline.push(`producer: put(1) -> ${res1}`)

    timeline.push('producer: 2 >')
    const res2 = await ch.put(2)
    timeline.push(`producer: put(2) -> ${res2}`)

    timeline.push('producer: 3 >')
    const res3 = await ch.put(3)
    timeline.push(`producer: put(3) -> ${res3}`)

    timeline.push('producer: 4 >')
    const res4 = await ch.put(4)
    timeline.push(`producer: put(4) -> ${res4}`)

    timeline.push('producer: 5 >')
    const res5 = await ch.put(5)
    timeline.push(`producer: put(5) -> ${res5}`)

    timeline.push('producer: 6 >')
    const res6 = await ch.put(6)
    timeline.push(`producer: put(6) -> ${res6}`)

    timeline.push('producer: 7 >')
    const res7 = await ch.put(7)
    timeline.push(`producer: put(7) -> ${res7}`)

    timeline.push('producer: 98 >')
    const res98 = await ch.put(98)
    timeline.push(`producer: put(98) -> ${res98}`)

    timeline.push('producer: 99 >')
    const res99 = await chan.put(99)
    timeline.push(`producer: put(99) -> ${res99}`)

    timeline.push('producer end.')
  }

  const chan = new AsyncChannel<number>({ bufferLength: 2 })

  await Promise.all([consumer(chan), producer(chan)])

  assert.deepEqual(
    timeline,
    //
    [
      'consumer: start',
      'producer: start',

      'producer: 1 >',
      'consumer: > 1',
      'producer: put(1) -> true',

      'producer: 2 >',
      'consumer: > 2',
      'producer: put(2) -> true',

      'producer: 3 >',
      'consumer: > 3',
      'producer: put(3) -> true',

      'producer: 4 >',
      'consumer: > 4',
      'consumer: timeout 10',
      'producer: put(4) -> true',

      'producer: 5 >',
      'producer: put(5) -> true',

      'producer: 6 >',
      'producer: put(6) -> true',

      'producer: 7 >',
      'producer: put(7) -> true',

      'producer: 98 >',

      'consumer: > 5',
      'consumer: timeout 10',

      'producer: put(98) -> true',
      'producer: 99 >',

      'consumer: > 6',
      'consumer: timeout 10',

      'producer: put(99) -> true',

      'producer end.',

      'consumer: > 7',
      'consumer: timeout 10',

      'consumer: break',

      'consumer: chan close',

      'consumer: end.'
    ]
  )
})

test('AsyncChannel (first take)', async () => {
  const chan = new AsyncChannel<number>()

  const consumer = (async () => {
    for await (const it of chan) {
      setTimeoutAsync(it + 10)
    }

    assert.ok('DONE')
  })()

  setTimeout(async () => {
    await chan.put(1)
    setTimeout(async () => {
      await chan.put(2)
      await chan.put(3)
      await chan.flush()
      chan.close()
    })
  }, 100)

  assert.ok(consumer)
})

test('AsyncChannel (errors)', async () => {
  const chan = new AsyncChannel<number>()

  const producer = async () => {
    await chan.put(1)
    await chan.put(2)
    await chan.put(3)
    setTimeout(() => chan.close(new Error('Channel error')), 0)
    await chan.put(4)
  }

  producer()

  try {
    for await (const num of chan) {
      assert.ok(typeof num === 'number')
    }
  } catch (err) {
    assert.ok(err instanceof Error)
    assert.equal(err.message, 'Channel error')
    return
  }

  assert.fail('Error expected')
})
