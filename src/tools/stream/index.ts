import { TableRow } from '../../types.js'

export * from './ChunkTransform.js'
export * from './PivotHeaderTransform.js'
export * from './FlattenTransform.js'

export function cloneChunk(chunk: TableRow[]): TableRow[] {
  return chunk.map(row => [...row])
}
