import type { AsyncChannel, TableRow } from '../../index.js'

export async function pipeAsyncIterableToChannel(
  sourceIter: AsyncIterable<TableRow[]>,
  targetChan: AsyncChannel<TableRow[]>,
  closeChannelOnSourceEnd: boolean = false
) {
  for await (const chunk of sourceIter) {
    if (targetChan.isClosed()) break
    await targetChan.put(chunk)
  }

  await targetChan.flush()

  if (closeChannelOnSourceEnd && !targetChan.isClosed()) {
    targetChan.close()
  }
}
