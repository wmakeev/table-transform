import assert from 'assert'
import { TransformStepError } from '../errors/index.js'
import {
  TableChunksAsyncIterable,
  TableChunksTransformer,
  TableRow,
  TableTransfromConfig,
  createTableTransformer,
  transforms as tf
} from '../index.js'
import { getNormalizedHeaderRow } from '../tools/header/index.js'

export interface SplitInParams {
  splitColumns: string[]
  transformConfig: TableTransfromConfig
}

const TRANSFORM_NAME = 'SplitIn'

const getRowsComparator = (columnIndexes: number[]) => {
  return (a: TableRow, b: TableRow): boolean => {
    return columnIndexes.every(index => a[index] === b[index])
  }
}

class AsyncGeneratorProxy implements TableChunksAsyncIterable {
  #sourceGenerator: AsyncGenerator<TableRow[], any, unknown> | null = null

  #rowsComparator: (a: TableRow, b: TableRow) => boolean

  #lastChunkRest: TableRow[] = []

  #suspended = false

  constructor(
    splitColumns: string[],
    private source: TableChunksAsyncIterable
  ) {
    this.#sourceGenerator = source[Symbol.asyncIterator]()

    const splitColumnsSet = new Set(splitColumns)

    const splitColumnIndexes = source
      .getHeader()
      .filter(h => !h.isDeleted && splitColumnsSet.has(h.name))
      .map(h => h.index)

    this.#rowsComparator = getRowsComparator(splitColumnIndexes)
  }

  async next() {
    if (this.#suspended) {
      return { done: true as const, value: undefined }
    }

    const curChunk = [...this.#lastChunkRest]

    if (this.#sourceGenerator != null) {
      const nextResult = await this.#sourceGenerator.next()

      if (nextResult.done === true) {
        this.#sourceGenerator = null
      } else {
        curChunk.push(...nextResult.value)
      }
    }

    // Iterator is ended
    if (curChunk[0] == null) {
      return { done: true as const, value: undefined }
    }

    this.#lastChunkRest = []

    const chunkFirstRow = curChunk[0]

    const splitIndex = curChunk.findIndex(
      row => !this.#rowsComparator(chunkFirstRow, row)
    )

    if (splitIndex === -1) {
      return { done: false, value: curChunk }
    }

    const curChunkHead = curChunk.slice(0, splitIndex)
    const curChunkTail = curChunk.slice(splitIndex)

    this.#lastChunkRest = curChunkTail

    this.suspend()

    return { done: false, value: curChunkHead }
  }

  return(value: any) {
    assert.ok(this.#sourceGenerator)
    return this.#sourceGenerator.return(value)
  }

  throw(e: any) {
    assert.ok(this.#sourceGenerator)
    return this.#sourceGenerator.throw(e)
  }

  getHeader() {
    return this.source.getHeader()
  }

  [Symbol.asyncIterator]() {
    return this
  }

  suspend() {
    this.#suspended = true
  }

  contine() {
    this.#suspended = false
  }

  isDone() {
    return this.#sourceGenerator == null
  }
}

/**
 * SplitIn
 */
export const splitIn = (params: SplitInParams): TableChunksTransformer => {
  const { splitColumns } = params

  return source => {
    const srcHeader = source.getHeader()

    // Drop headers from splits
    const transformConfig: TableTransfromConfig = {
      ...params.transformConfig,
      outputHeader: {
        ...params.transformConfig.outputHeader,
        skip: true
      }
    }

    //#region #jsgf360l

    // TODO Этот блок копипаста из createTableTransformer.
    // Явно есть просчеты в архитектуре, раз приходится городить такие костыли.
    // Фактически сейчас без подобных костылей не получить заголовки по конфигурации.
    // Возможно это нормально? Ведь в этом модуле приходится несколько раз вызывать
    // трансформацию с одинаковым конфигом (все ли созданные трансформации можно
    // вызывать повторно?), но выглядить это запутанно и очень сложно отлаживать.

    const transforms_ = [...(transformConfig.transforms ?? [])]

    // Ensure all forsed columns exist and select
    if (transformConfig.outputHeader?.forceColumns != null) {
      transforms_.push(
        tf.column.select({
          columns: transformConfig.outputHeader.forceColumns,
          addMissingColumns: true
        })
      )
    }

    transforms_.push(tf.normalize({ immutable: false }))

    let tableSource = source

    // Chain transformations
    for (const transform of transforms_) {
      tableSource = transform(tableSource)
    }

    const transfomedHeader = tableSource.getHeader()
    //#endregion

    async function* getTransformedSourceGenerator() {
      const normalizedHeaderColumns = getNormalizedHeaderRow(srcHeader)

      // TODO Выделить в отдельный хелпер?
      //#region Check columns in source
      const headerColumnsSet = new Set(normalizedHeaderColumns)

      const notFoundColumns = []

      for (const col of splitColumns) {
        if (!headerColumnsSet.has(col)) notFoundColumns.push(col)
      }

      if (notFoundColumns.length !== 0) {
        throw new TransformStepError(
          `Columns not found - ${notFoundColumns.map(col => `"${col}"`).join(', ')}`,
          TRANSFORM_NAME
        )
      }
      //#endregion

      const iteratorProxy = new AsyncGeneratorProxy(splitColumns, source)

      while (true) {
        yield* createTableTransformer(transformConfig)(iteratorProxy)
        if (iteratorProxy.isDone()) break
        iteratorProxy.contine()
      }
    }

    return {
      getHeader: () => transfomedHeader,
      [Symbol.asyncIterator]: getTransformedSourceGenerator
    }
  }
}
