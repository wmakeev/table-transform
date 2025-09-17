import assert from 'assert'
import {
  Context,
  TableChunksSource,
  TableTransformer,
  TableTransformConfig,
  transforms as tf,
  TableChunksIterable
} from '../index.js'
import { getInitialTableSource } from './getInitialTableSource.js'

export * from './Context.js'

export function createTableTransformer(
  config: TableTransformConfig
): TableTransformer {
  const {
    transforms = [],
    inputHeader,
    outputHeader,
    errorHandle,
    context
  } = config

  return async function* (source: TableChunksIterable | TableChunksSource) {
    let tableSource: TableChunksSource | null = null
    let transformedTableColumns: string[] | null = null

    try {
      tableSource =
        'getHeader' in source
          ? source
          : await getInitialTableSource({
              inputHeaderOptions: inputHeader,
              chunkedRowsIterable: source,
              context
            })

      //#region #jsgf360l
      const transforms_ = [...transforms]

      // Ensure all forced columns exist and select
      if (outputHeader?.forceColumns != null) {
        transforms_.push(
          tf.column.select({
            columns: outputHeader.forceColumns,
            addMissingColumns: true
          })
        )
      }

      transforms_.push(tf.normalize({ immutable: false }))

      let maxHeadersCount = 0

      // Chain transformations
      for (const transform of transforms_) {
        tableSource = transform(tableSource)
        const curHeadersCount = tableSource.getHeader().length
        if (curHeadersCount > maxHeadersCount) maxHeadersCount = curHeadersCount
      }

      // TODO Подумать как использовать maxHeadersCount для оптимизации, задавая
      // размер массива с указанным значением заранее на входе.

      transformedTableColumns = tableSource.getHeader().map(h => h.name)
      //#endregion

      if (outputHeader?.skip !== true) {
        // header is just normalized
        yield [[...transformedTableColumns]]
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
        transformedTableColumns ?? outputHeader?.forceColumns

      // TODO Подумать над тем как передавать разные типы ошибок
      const errorInfo = {
        name: err.name,
        code: (err as any).code,
        message: err.message,
        stepName: (err as any).stepName,
        header: (err as any).header,
        chunk: (err as any).chunk,
        row: (err as any).row,
        rowIndex: (err as any).rowIndex,
        rowRecord: (err as any).getRowRecord?.(),
        expression: (err as any).expression,
        column: (err as any).column,
        columnIndex: (err as any).columnIndex
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

      // TODO Было бы не плохо иметь возможность передать ошибку в переменную,
      // а не в колонку. Но это можно сделать сейчас только через контекст
      // трансформации. Вероятно тут нужен как раз этот глобальный контекст.

      yield* createTableTransformer({
        transforms: _transforms,
        outputHeader: {
          skip:
            sourceResultColumns != null || outputHeader?.forceColumns != null
        },
        context: new Context(context)
      })([[[errorHandle.errorColumn], [errorInfo]]])
    }
  }
}
