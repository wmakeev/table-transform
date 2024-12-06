import { TableRow } from '../../types/index.js'

export * from './chunkSourceFromChannel.js'
export * from './ChunkTransform.js'
export * from './FlattenTransform.js'
export * from './pipeAsyncIterableToChannel.js'
export * from './PivotHeaderTransform.js'

export function cloneChunk(chunk: TableRow[]): TableRow[] {
  return chunk.map(row => [...row])
}
