import assert from 'assert'
import {
  ColumnHeader,
  TableChunksAsyncIterable,
  TableRow,
  TableTransformer,
  TableTransfromConfig,
  getTransformedSource,
  transforms as tf
} from '../index.js'
import { getInitialTableSource } from './getInitialTableSource.js'

export function createTableTransformer(
  config: TableTransfromConfig
): TableTransformer {
  const { transforms = [], inputHeader, outputHeader, errorHandle } = config

  return async function* (
    source:
      | Iterable<TableRow[]>
      | AsyncIterable<TableRow[]>
      | TableChunksAsyncIterable
  ) {
    let tableSource: TableChunksAsyncIterable | null = null
    let transfomedTableHeader: ColumnHeader[] | null = null

    try {
      tableSource =
        'getHeader' in source
          ? source
          : await getInitialTableSource({
              inputHeaderOptions: inputHeader,
              chunkedRowsIterable: source
            })

      const transforms_ = [...transforms]

      // Ensure all forsed columns exist and select
      if (outputHeader?.forceColumns != null) {
        transforms_.push(
          tf.column.select({
            columns: outputHeader.forceColumns,
            addMissingColumns: true
          })
        )
      }

      transforms_.push(tf.normalize({ immutable: false }))

      // Chain transformations
      tableSource = getTransformedSource(tableSource, transforms_)

      transfomedTableHeader = tableSource.getHeader()

      if (outputHeader?.skip !== true) {
        // header is just normalized
        yield [transfomedTableHeader.map(h => h.name)]
      }

      yield* tableSource
    } catch (err) {
      // #dhf042pf
      assert.ok(err instanceof Error)

      // TODO Вероятно нужно ловить только TransformError и наследников

      if (errorHandle == null) {
        throw err
      }

      const sourceResultHeaders =
        outputHeader?.forceColumns ??
        (transfomedTableHeader != null
          ? tableSource
              ?.getHeader()
              .filter(h => !h.isDeleted)
              .map(h => h.name)
          : undefined)

      // TODO Сделать специальный класс ошибки
      const errorInfo = {
        name: err.name,
        code: (err as any).code,
        message: err.message
      }

      const _transforms = [...(errorHandle.transforms ?? [])]

      if (sourceResultHeaders) {
        _transforms.push(
          tf.column.select({
            columns: sourceResultHeaders,
            addMissingColumns: true
          })
        )
      }

      yield* createTableTransformer({
        transforms: _transforms,
        outputHeader: {
          skip:
            transfomedTableHeader != null || outputHeader?.forceColumns != null
        }
      })([[[errorHandle.errorColumn], [errorInfo]]])
    }
  }
}
