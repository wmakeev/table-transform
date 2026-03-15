import { Context, createTableTransformer } from '../table-transformer/index.js'
import {
  TableChunksTransformer,
  TableRow,
  TableTransformConfig
} from '../types/index.js'
import { select } from './column/select.js'
import { TransformBaseParams } from './index.js'
import { normalize } from './internal/normalize.js'

// TODO #399hajv97

export interface PrependParams extends TransformBaseParams {
  /**
   * Initial table data
   */
  table: TableRow[]

  /**
   * The config of transformation through which table rows is passed.
   */
  transformConfig: TableTransformConfig
}

// const TRANSFORM_NAME = 'Prepend'

/**
 * Prepend
 */
export const prepend = (params: PrependParams): TableChunksTransformer => {
  const { transformConfig, table } = params

  return sourceIn => {
    const columns = sourceIn
      .getTableHeader()
      .flatMap(h => (h.isDeleted ? [] : h.name))

    const prependTransformer = createTableTransformer({
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

    const sourceOut = normalize()(sourceIn)

    return {
      ...sourceIn,
      [Symbol.asyncIterator]: async function* () {
        for await (const batch of prependTransformer([table])) {
          yield batch
        }

        for await (const batch of sourceOut) {
          yield batch
        }
      }
    }
  }
}
