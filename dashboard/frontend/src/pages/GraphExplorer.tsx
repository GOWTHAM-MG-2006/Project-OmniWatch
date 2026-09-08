import { useState, useCallback } from 'react'
import { neo4jQuery, neo4jSchema } from '../api/client'
import type { Neo4jQueryResult, Neo4jSchemaInfo } from '../api/client'
import { AuthGate } from '../components/AuthGate'

export function GraphExplorer() {
  const [query, setQuery] = useState('')
  const [result, setResult] = useState<Neo4jQueryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [schema, setSchema] = useState<Neo4jSchemaInfo | null>(null)
  const [schemaLoading, setSchemaLoading] = useState(false)

  const handleExecute = useCallback(async () => {
    if (!query.trim()) return
    setLoading(true)
    setError(null)
    try {
      const res = await neo4jQuery(query.trim(), 200)
      if (res.error) {
        setError(res.error)
        setResult(null)
      } else {
        setResult(res)
        setError(null)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Query failed')
      setResult(null)
    } finally {
      setLoading(false)
    }
  }, [query])

  const handleLoadSchema = useCallback(async () => {
    setSchemaLoading(true)
    try {
      const res = await neo4jSchema()
      setSchema(res)
    } catch {
      setSchema(null)
    } finally {
      setSchemaLoading(false)
    }
  }, [])

  return (
    <AuthGate service="neo4j" onAuth={handleLoadSchema}>
      <div className="p-4 flex flex-col gap-4">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">Neo4j Console</h1>
          <p className="text-xs text-text-muted font-mono">POST /api/neo4j/query — {result?.row_count ?? 0} rows</p>
        </div>

        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-3 card rounded-lg border border-[#2a2a2a] overflow-hidden">
            <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">
              Schema
              {!schema && (
                <button onClick={handleLoadSchema} className="ml-2 text-accent-cyan hover:underline">
                  {schemaLoading ? 'Loading...' : 'Load'}
                </button>
              )}
            </div>
            {schema ? (
              <div className="p-3 space-y-3 text-xs">
                <div>
                  <div className="text-[10px] uppercase tracking-widest text-text-muted font-mono mb-1">Labels</div>
                  <div className="flex flex-wrap gap-1">
                    {schema.labels.map((l) => (
                      <span key={l} className="px-2 py-0.5 rounded bg-accent-cyan/10 text-accent-cyan font-mono text-[10px]">{l}</span>
                    ))}
                  </div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-widest text-text-muted font-mono mb-1">Relationships</div>
                  <div className="flex flex-wrap gap-1">
                    {schema.relationshipTypes.map((r) => (
                      <span key={r} className="px-2 py-0.5 rounded bg-accent-violet/10 text-accent-violet font-mono text-[10px]">{r}</span>
                    ))}
                  </div>
                </div>
                <div>
                  <div className="text-[10px] uppercase tracking-widest text-text-muted font-mono mb-1">Properties</div>
                  <div className="flex flex-wrap gap-1">
                    {schema.propertyKeys.map((p) => (
                      <span key={p} className="px-2 py-0.5 rounded bg-accent-amber/10 text-accent-amber font-mono text-[10px]">{p}</span>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="p-4 text-xs text-text-muted">Click Load to view schema</div>
            )}
          </div>

          <div className="col-span-9 flex flex-col gap-4">
            <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden">
              <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">Cypher Query</div>
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) handleExecute() }}
                placeholder="MATCH (n) RETURN n LIMIT 10"
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

            {result && (
              <div className="card rounded-lg border border-[#2a2a2a] overflow-hidden">
                <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">
                  Results · {result.row_count} rows {result.truncated ? '(truncated)' : ''}
                </div>
                {result.columns.length === 0 ? (
                  <div className="p-6 text-center text-sm text-text-muted">Query returned no columns</div>
                ) : (
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
                            {result.columns.map((col) => {
                              const val = row[col]
                              const display = val === null || val === undefined
                                ? <span className="text-text-muted italic">NULL</span>
                                : typeof val === 'object'
                                  ? JSON.stringify(val)
                                  : String(val)
                              return (
                                <td key={col} className="px-3 py-1.5 font-mono truncate max-w-[200px]" title={typeof val === 'object' ? JSON.stringify(val) : String(val ?? '')}>
                                  {display}
                                </td>
                              )
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </AuthGate>
  )
}
