import { useState, useCallback } from 'react'
import { clickhouseQuery, clickhouseTables, clickhouseSchema, apiError } from '../api/client'
import type { ClickHouseQueryResult, ClickHouseTableInfo, ClickHouseSchemaColumn } from '../api/client'
import { AuthGate } from '../components/AuthGate'

export function DataExplorer() {
  const [query, setQuery] = useState('')
  const [result, setResult] = useState<ClickHouseQueryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [tables, setTables] = useState<ClickHouseTableInfo[]>([])
  const [tablesLoading, setTablesLoading] = useState(false)
  const [tablesLoaded, setTablesLoaded] = useState(false)

  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [schema, setSchema] = useState<ClickHouseSchemaColumn[]>([])
  const [schemaLoading, setSchemaLoading] = useState(false)

  const handleExecute = useCallback(async () => {
    if (!query.trim()) return
    setLoading(true)
    setError(null)
    try {
      const res = await clickhouseQuery(query.trim(), 200)
      if (res.error) {
        setError(res.error)
        setResult(null)
      } else {
        setResult(res)
        setError(null)
      }
    } catch (e) {
      setError(apiError(e, 'Query failed'))
      setResult(null)
    } finally {
      setLoading(false)
    }
  }, [query])

  const handleLoadTables = useCallback(async () => {
    setTablesLoading(true)
    try {
      const res = await clickhouseTables()
      setTables(res.tables)
      setTablesLoaded(true)
    } catch {
      setTables([])
    } finally {
      setTablesLoading(false)
    }
  }, [])

  const handleSelectTable = useCallback(async (name: string) => {
    setSelectedTable(name)
    setSchemaLoading(true)
    try {
      const res = await clickhouseSchema(name)
      setSchema(res.columns)
    } catch {
      setSchema([])
    } finally {
      setSchemaLoading(false)
    }
  }, [])

  return (
    <AuthGate service="clickhouse" onAuth={handleLoadTables}>
      <div className="p-4 flex flex-col gap-4">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">ClickHouse Console</h1>
          <p className="text-xs text-text-muted font-mono">POST /api/clickhouse/query — {result?.row_count ?? 0} rows</p>
        </div>

        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-3 card rounded-lg border border-[#2a2a2a] overflow-hidden">
            <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">
              Tables
              {!tablesLoaded && (
                <button onClick={handleLoadTables} className="ml-2 text-accent-cyan hover:underline">
                  {tablesLoading ? 'Loading...' : 'Load'}
                </button>
              )}
            </div>
            <div className="divide-y divide-[#1e1e1e] max-h-[500px] overflow-auto">
              {tables.map((t) => (
                <button
                  key={t.name}
                  onClick={() => handleSelectTable(t.name)}
                  className={`w-full text-left px-3 py-2 text-xs font-mono hover:bg-[#1a1a1a] transition-colors ${selectedTable === t.name ? 'bg-accent-cyan/10 border-l-2 border-accent-cyan' : 'border-l-2 border-transparent'}`}
                >
                  <div className={`truncate ${selectedTable === t.name ? 'text-accent-cyan' : 'text-text-primary'}`}>{t.name}</div>
                  <div className="text-[10px] text-text-muted">{t.engine ?? '—'}{typeof t.row_count === 'number' ? ` · ${t.row_count.toLocaleString()} rows` : ''}</div>
                </button>
              ))}
            </div>
          </div>

          <div className="col-span-9 flex flex-col gap-4">
            <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden">
              <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">SQL Query</div>
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) handleExecute() }}
                placeholder="SELECT * FROM metrics LIMIT 10"
                className="w-full h-32 p-3 text-sm font-mono bg-[#0a0a0f] text-text-primary resize-none focus:outline-none"
              />
              <div className="px-3 py-2 flex items-center gap-2 border-t border-[#2a2a2a]">
                <button
                  onClick={handleExecute}
                  disabled={loading || !query.trim()}
                  className="px-4 py-1.5 text-xs font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40"
                >
                  {loading ? 'Running...' : 'Execute (Ctrl+Enter)'}
                </button>
                {error && <span className="text-xs text-red-400 font-mono truncate max-w-[400px]">{error}</span>}
              </div>
            </div>

            {selectedTable && (
              <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden">
                <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">
                  Schema: {selectedTable}
                </div>
                {schemaLoading ? (
                  <div className="p-4 text-xs text-text-muted animate-pulse">Loading schema...</div>
                ) : (
                  <div className="overflow-auto max-h-[200px]">
                    <table className="w-full text-xs">
                      <thead className="sticky top-0 bg-[#1a1a1a] text-[10px] uppercase tracking-widest text-text-muted font-mono">
                        <tr>
                          <th className="text-left px-3 py-2 font-normal">Column</th>
                          <th className="text-left px-3 py-2 font-normal">Type</th>
                          <th className="text-left px-3 py-2 font-normal">Default</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-[#1e1e1e]">
                        {schema.map((col) => (
                          <tr key={col.name} className="hover:bg-[#141618]">
                            <td className="px-3 py-1.5 font-mono text-accent-cyan">{col.name}</td>
                            <td className="px-3 py-1.5 font-mono text-text-muted">{col.type}</td>
                            <td className="px-3 py-1.5 font-mono text-text-muted">{col.default_expression || '—'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {result && (
              <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden">
                <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">
                  Results · {result.row_count} rows {result.truncated ? '(truncated)' : ''}
                </div>
                <div className="overflow-auto max-h-[400px]">
                  <table className="w-full text-xs">
                    <thead className="sticky top-0 bg-[#1a1a1a] text-[10px] uppercase tracking-widest text-text-muted font-mono">
                      <tr>
                        {result.columns.map((col) => (
                          <th key={col} className="text-left px-3 py-2 font-normal">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#1e1e1e]">
                      {result.rows.map((row, i) => (
                        <tr key={i} className="hover:bg-[#141618]">
                          {row.map((cell, j) => (
                            <td key={j} className="px-3 py-1.5 font-mono truncate max-w-[200px]" title={String(cell ?? '')}>
                              {cell === null ? <span className="text-text-muted italic">NULL</span> : String(cell)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </AuthGate>
  )
}
