import { Context, createTableTransformer } from '../table-transformer/index.js'
import {
  TableChunksTransformer,
  TableRow,
  TableTransformConfig
} from '../types/index.js'
import { select } from './column/select.js'
import { TransformBaseParams } from './index.js'
import { normalize } from './internal/normalize.js'

// TODO Есть общая логика с prepend #399hajv97
// Метод почти ничем не отличается от prepend

export interface AppendParams extends TransformBaseParams {
  /**
   * Initial table data
   */
  table: TableRow[]

  /**
   * The config of transformation through which table rows is passed.
   */
  transformConfig: TableTransformConfig
}

// const TRANSFORM_NAME = 'Append'

/**
 * Append
 */
export const append = (params: AppendParams): TableChunksTransformer => {
  const { transformConfig, table } = params

  return sourceIn => {
    const columns = sourceIn
      .getTableHeader()
      .flatMap(h => (h.isDeleted ? [] : h.name))

    // TODO Duplicate: Эта логика повторяется во многих местах.
    const appendTransformer = createTableTransformer({
      context: new Context(sourceIn.getContext()),
      ...transformConfig,
      outputHeader: {
        ...transformConfig.outputHeader,
        skip: true
      },
      transforms: [
        ...(transformConfig.transforms ?? []),
        select({
          columns,
          addMissingColumns: true
        }),
        normalize()
      ]
    })

    // TODO Нужно какое-то общее правило почему sourceOut расположен тут, а скажем
    // не внутри Symbol.asyncIterator. Есть разница? Потому что постоянно возникает
    // такой вопрос при реализации.

    const sourceOut = normalize()(sourceIn)

    return {
      ...sourceIn,
      [Symbol.asyncIterator]: async function* () {
        for await (const batch of sourceOut) {
          yield batch
        }

        for await (const batch of appendTransformer([table])) {
          yield batch
        }
      }
    }
  }
}
