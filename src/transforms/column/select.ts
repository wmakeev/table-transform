import { DataRowChunkTransformer, TableHeaderMeta } from '../../index.js'

export interface SelectColumnsParams {
  /** Columns that should be selected */
  columns: string[]

  keepSrcColumnsOrder?: boolean

  eraseData?: boolean
}

export const select = (
  params: SelectColumnsParams
): DataRowChunkTransformer => {
  const { columns: selectedColumns, keepSrcColumnsOrder } = params

  if (selectedColumns.length === 0) {
    throw new Error('Columns to select not specified')
  }

  const selectedColumnsSet = new Set(selectedColumns)

  if (selectedColumns.length !== selectedColumnsSet.size) {
    throw new Error(`Select columns values has duplicates`)
  }

  let selectedColumnsMeta: TableHeaderMeta | null = null
  let selectedColumnsSrcIndexesSet: Set<number> | null = null

  const transformer: DataRowChunkTransformer = async ({
    header,
    rows,
    rowLength
  }) => {
    //#region Select selected columns
    if (selectedColumnsMeta === null) {
      selectedColumnsMeta = [] as TableHeaderMeta

      // Keep original columns order
      if (keepSrcColumnsOrder === true) {
        for (const head of header) {
          if (selectedColumnsSet.has(head.name)) {
            selectedColumnsMeta.push(head)
          }
        }
      }

      // Reorder columns
      else {
        const notFound: string[] = []

        // Get selected headers
        for (const colName of selectedColumns) {
          const selectedColHeaders = header.filter(h => h.name === colName)

          if (selectedColHeaders.length) {
            selectedColumnsMeta.push(...selectedColHeaders)
          } else {
            notFound.push(colName)
          }
        }

        if (notFound.length) {
          throw new Error(
            `Columns not found and can't be selected: ${notFound.map(c => `"${c}"`).join(', ')}`
          )
        }
      }

      selectedColumnsSrcIndexesSet = new Set(
        selectedColumnsMeta.map(h => h.srcIndex)
      )
    }
    //#endregion

    if (params.eraseData === true) {
      rows.forEach(row => {
        row.forEach((_, index) => {
          if (!selectedColumnsSrcIndexesSet!.has(index)) {
            row[index] = null
          }
        })
      })
    }

    return {
      header: selectedColumnsMeta,
      rows,
      rowLength
    }
  }

  return transformer
}
