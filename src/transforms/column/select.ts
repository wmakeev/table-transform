import assert from 'assert'
import { ColumnHeader, TableChunksTransformer } from '../../index.js'

export interface SelectColumnsParams {
  /** Columns that should be selected */
  columns: string[]

  keepSrcColumnsOrder?: boolean

  eraseData?: boolean
}

export const select = (params: SelectColumnsParams): TableChunksTransformer => {
  const { columns: selectedColumns, keepSrcColumnsOrder = false } = params

  if (selectedColumns.length === 0) {
    throw new Error('Columns to select not specified')
  }

  const selectedColumnsNamesSet = new Set(selectedColumns)

  if (selectedColumns.length !== selectedColumnsNamesSet.size) {
    throw new Error(`Select columns values has duplicates`)
  }

  return async ({ header, getSourceGenerator }) => {
    //#region Ensure selected headers exist
    const notDeletedHeaders = header.filter(h => !h.isDeleted)

    const notFoundColumnsNamesSet = new Set(notDeletedHeaders.map(h => h.name))

    notDeletedHeaders.forEach(h => {
      notFoundColumnsNamesSet.delete(h.name)
    })

    if (notFoundColumnsNamesSet.size !== 0) {
      throw new Error(
        "Columns not found and can't be selected: " +
          [...notFoundColumnsNamesSet.values()].map(c => `"${c}"`).join(', ')
      )
    }
    //#endregion

    // Mark not selected columns as deleted
    let transformedHeader: ColumnHeader[] = header.map(h => {
      return !h.isDeleted && selectedColumnsNamesSet.has(h.name)
        ? h
        : { ...h, isDeleted: true }
    })

    // Reorder columns
    if (!keepSrcColumnsOrder) {
      const orderedHeaders: ColumnHeader[] = []

      // Get selected headers
      for (const colName of selectedColumns) {
        const colNameHeaders = transformedHeader.filter(
          h => !h.isDeleted && h.name === colName
        )

        if (colNameHeaders.length) {
          orderedHeaders.push(...colNameHeaders)
        }
      }

      transformedHeader = [
        ...orderedHeaders,

        // Other headers is deleted
        ...transformedHeader.filter(h => h.isDeleted)
      ]
    }

    assert.equal(header.length, transformedHeader.length)

    /** Indexes of all selected columns */
    const selectedColumnsSrcIndexesSet = new Set(
      transformedHeader.filter(h => !h.isDeleted).map(h => h.index)
    )

    async function* getTransformedSourceGenerator() {
      for await (const chunk of getSourceGenerator()) {
        if (params.eraseData === true) {
          chunk.forEach(row => {
            row.forEach((_, index) => {
              if (!selectedColumnsSrcIndexesSet.has(index)) {
                row[index] = null
              }
            })
          })
        }

        yield chunk
      }
    }

    return {
      header: transformedHeader,
      getSourceGenerator: getTransformedSourceGenerator
    }
  }
}
