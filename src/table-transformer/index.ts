import assert from 'assert'
import {
  TableChunksAsyncIterable,
  TableRow,
  TableTransformer,
  TableTransfromConfig,
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
    let transfomedTableColumns: string[] | null = null

    try {
      tableSource =
        'getHeader' in source
          ? source
          : await getInitialTableSource({
              inputHeaderOptions: inputHeader,
              chunkedRowsIterable: source
            })

      //#region #jsgf360l
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
      for (const transform of transforms_) {
        tableSource = transform(tableSource)
      }

      transfomedTableColumns = tableSource.getHeader().map(h => h.name)
      //#endregion

      if (outputHeader?.skip !== true) {
        // header is just normalized
        yield [[...transfomedTableColumns]]
      }

      yield* tableSource
    } catch (err) {
      // #dhf042pf
      assert.ok(err instanceof Error)

      // TODO Вероятно нужно ловить только TransformError и наследников

      if (errorHandle == null) {
        throw err
      }

      const sourceResultColumns =
        transfomedTableColumns ?? outputHeader?.forceColumns

      // TODO Сделать специальный класс ошибки
      const errorInfo = {
        name: err.name,
        code: (err as any).code,
        message: err.message
      }

      const _transforms = [...(errorHandle.transforms ?? [])]

      if (sourceResultColumns) {
        _transforms.push(
          tf.column.select({
            columns: sourceResultColumns,
            addMissingColumns: true
          })
        )
      }

      yield* createTableTransformer({
        transforms: _transforms,
        outputHeader: {
          skip:
            sourceResultColumns != null || outputHeader?.forceColumns != null
        }
      })([[[errorHandle.errorColumn], [errorInfo]]])
    }
  }
}
