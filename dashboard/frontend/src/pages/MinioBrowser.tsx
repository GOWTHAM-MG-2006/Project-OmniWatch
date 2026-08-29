import { useState, useEffect, useCallback } from 'react'
import { fetchMinioBuckets, fetchMinioObjects } from '../api/client'
import type { MinioBucket, MinioObject } from '../api/client'

function formatSize(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  let i = 0
  let v = bytes
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

export function MinioBrowser() {
  const [buckets, setBuckets] = useState<MinioBucket[] | null>(null)
  const [bucketsLoading, setBucketsLoading] = useState(true)
  const [bucketsError, setBucketsError] = useState<string | null>(null)

  const [selectedBucket, setSelectedBucket] = useState<string | null>(null)
  const [prefix, setPrefix] = useState('')
  const [objects, setObjects] = useState<MinioObject[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const limit = 50
  const [objectsLoading, setObjectsLoading] = useState(false)
  const [objectsError, setObjectsError] = useState<string | null>(null)

  const loadBuckets = useCallback(async () => {
    setBucketsLoading(true)
    setBucketsError(null)
    try {
      const res = await fetchMinioBuckets()
      if (res.error) {
        setBucketsError(res.error)
        setBuckets([])
      } else {
        setBuckets(res.buckets)
      }
    } catch (e) {
      setBucketsError(e instanceof Error ? e.message : 'Failed to load buckets')
      setBuckets([])
    } finally {
      setBucketsLoading(false)
    }
  }, [])

  useEffect(() => {
    loadBuckets()
  }, [loadBuckets])

  const loadObjects = useCallback(async (bucket: string, pfx: string, off: number) => {
    setObjectsLoading(true)
    setObjectsError(null)
    try {
      const res = await fetchMinioObjects({ bucket, prefix: pfx, limit, offset: off })
      setObjects(res.objects)
      setTotal(res.total)
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to load objects'
      const isAxios = (e as unknown as { response?: { data?: { error?: string } } })?.response?.data?.error
      setObjectsError(isAxios || msg)
      setObjects([])
      setTotal(0)
    } finally {
      setObjectsLoading(false)
    }
  }, [])

  const handleSelectBucket = (name: string) => {
    setSelectedBucket(name)
    setPrefix('')
    setOffset(0)
    loadObjects(name, '', 0)
  }

  const handleRefresh = () => {
    if (selectedBucket) {
      loadObjects(selectedBucket, prefix, offset)
    } else {
      loadBuckets()
    }
  }

  const handlePrefixSearch = () => {
    if (!selectedBucket) return
    setOffset(0)
    loadObjects(selectedBucket, prefix, 0)
  }

  const totalPages = Math.ceil(total / limit)
  const currentPage = Math.floor(offset / limit) + 1

  return (
    <div className="p-4 flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">MinIO Browser</h1>
          <p className="text-xs text-text-muted font-mono">Live buckets & objects via MinIO SDK — GET /api/minio/buckets · /api/minio/objects?bucket=</p>
        </div>
        <button
          onClick={() => { if (selectedBucket) { loadBuckets(); loadObjects(selectedBucket, prefix, offset) } else loadBuckets() }}
          className="px-3 py-1.5 text-xs font-mono rounded border border-[#2a2a2a] hover:border-accent-cyan/40 hover:text-accent-cyan transition-colors"
        >
          Refresh
        </button>
      </div>

      {bucketsLoading ? (
        <div className="card p-6 text-sm text-text-muted animate-pulse">Loading buckets...</div>
      ) : bucketsError ? (
        <div className="card p-6 rounded-lg border border-red-900/40 bg-red-950/20">
          <div className="text-sm text-red-400">Failed to load buckets: {bucketsError}</div>
          <div className="text-xs text-text-muted mt-1">Check MinIO is running (docker-compose up -d minio) and env MINIO_ENDPOINT.</div>
          <button onClick={loadBuckets} className="mt-3 px-3 py-1 text-xs rounded bg-red-900/30 hover:bg-red-900/50 text-red-300">Retry</button>
        </div>
      ) : buckets && buckets.length === 0 ? (
        <div className="card p-8 text-center rounded-lg border border-[#2a2a2a]">
          <div className="text-sm text-text-primary">No buckets found</div>
          <div className="text-xs text-text-muted mt-1">MinIO is reachable but has no buckets — run <code className="bg-[#1a1a1a] px-1 py-0.5 rounded">docker-compose up -d</code> and ensure bucket setup ran.</div>
          <button onClick={loadBuckets} className="mt-3 px-3 py-1 text-xs rounded border border-[#2a2a2a] hover:border-accent-cyan/30">Refresh</button>
        </div>
      ) : (
        <div className="grid grid-cols-12 gap-4">
          <div className="col-span-4 card rounded-lg border border-[#2a2a2a] overflow-hidden">
            <div className="px-3 py-2 text-[10px] uppercase tracking-widest text-text-muted font-mono border-b border-[#2a2a2a]">Buckets ({buckets?.length ?? 0})</div>
            <div className="divide-y divide-[#1e1e1e]">
              {buckets?.map((b) => (
                <button
                  key={b.name}
                  onClick={() => handleSelectBucket(b.name)}
                  className={`w-full text-left px-3 py-2.5 text-sm flex flex-col gap-0.5 hover:bg-[#1a1a1a] transition-colors ${selectedBucket === b.name ? 'bg-accent-cyan/10 border-l-2 border-accent-cyan' : 'border-l-2 border-transparent'}`}
                >
                  <span className={`font-mono text-xs truncate ${selectedBucket === b.name ? 'text-accent-cyan' : 'text-text-primary'}`}>{b.name}</span>
                  {b.creation_date && <span className="text-[10px] text-text-muted">{new Date(b.creation_date).toLocaleString()}</span>}
                </button>
              ))}
            </div>
          </div>

          <div className="col-span-8 card rounded-lg border border-[#2a2a2a] overflow-hidden flex flex-col">
            {!selectedBucket ? (
              <div className="flex-1 flex items-center justify-center p-8 text-center">
                <div>
                  <div className="text-sm text-text-muted">Select a bucket to browse objects</div>
                  <div className="text-xs text-text-muted/70 mt-1 font-mono">Real listing via MinIO list_objects — honest empty when bucket has no objects.</div>
                </div>
              </div>
            ) : (
              <>
                <div className="px-3 py-2 flex items-center gap-2 border-b border-[#2a2a2a]">
                  <span className="text-xs font-mono text-accent-cyan truncate">{selectedBucket}</span>
                  <span className="text-[10px] text-text-muted font-mono">{total} object{total !== 1 ? 's' : ''}</span>
                  <div className="ml-auto flex items-center gap-2">
                    <input
                      value={prefix}
                      onChange={(e) => setPrefix(e.target.value)}
                      onKeyDown={(e) => e.key === 'Enter' && handlePrefixSearch()}
                      placeholder="prefix filter"
                      className="px-2 py-1 text-xs bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono w-32 focus:outline-none focus:border-accent-cyan/40"
                    />
                    <button onClick={handlePrefixSearch} className="px-2 py-1 text-xs rounded bg-[#1e1e2e] hover:bg-[#2a2a4a]">Filter</button>
                    <button onClick={handleRefresh} className="px-2 py-1 text-xs rounded border border-[#2a2a2a] hover:border-accent-cyan/30">↻</button>
                  </div>
                </div>

                {objectsLoading ? (
                  <div className="p-6 text-sm text-text-muted animate-pulse">Loading objects...</div>
                ) : objectsError ? (
                  <div className="p-6 text-sm text-red-400">{objectsError}
                    <button onClick={handleRefresh} className="ml-3 px-2 py-1 text-xs rounded bg-red-900/30">Retry</button>
                  </div>
                ) : objects.length === 0 ? (
                  <div className="flex-1 flex items-center justify-center p-8 text-center">
                    <div>
                      <div className="text-sm text-text-primary">Bucket empty</div>
                      <div className="text-xs text-text-muted mt-1">No objects in <code className="bg-[#1a1a1a] px-1 rounded">{selectedBucket}</code>{prefix ? ` with prefix "${prefix}"` : ''} — run simulation to generate data.</div>
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="overflow-auto max-h-[420px]">
                      <table className="w-full text-xs">
                        <thead className="sticky top-0 bg-[#1a1a1a] text-[10px] uppercase tracking-widest text-text-muted font-mono">
                          <tr>
                            <th className="text-left px-3 py-2 font-normal">Name</th>
                            <th className="text-right px-3 py-2 font-normal">Size</th>
                            <th className="text-left px-3 py-2 font-normal">Last modified</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-[#1e1e1e]">
                          {objects.map((o) => (
                            <tr key={o.name} className="hover:bg-[#141618]">
                              <td className="px-3 py-1.5 font-mono truncate max-w-[360px]" title={o.name}>{o.name}</td>
                              <td className="px-3 py-1.5 text-right font-mono text-text-muted">{formatSize(o.size)}</td>
                              <td className="px-3 py-1.5 font-mono text-text-muted">{o.last_modified ? new Date(o.last_modified).toLocaleString() : '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    {total > limit && (
                      <div className="px-3 py-2 flex items-center justify-between border-t border-[#2a2a2a] text-xs font-mono">
                        <span className="text-text-muted">Page {currentPage} of {totalPages} · {total} total</span>
                        <div className="flex gap-2">
                          <button
                            disabled={offset === 0}
                            onClick={() => { const n = Math.max(0, offset - limit); setOffset(n); loadObjects(selectedBucket, prefix, n) }}
                            className="px-2 py-1 rounded border border-[#2a2a2a] disabled:opacity-30 hover:border-accent-cyan/30"
                          >
                            Prev
                          </button>
                          <button
                            disabled={offset + limit >= total}
                            onClick={() => { const n = offset + limit; setOffset(n); loadObjects(selectedBucket, prefix, n) }}
                            className="px-2 py-1 rounded border border-[#2a2a2a] disabled:opacity-30 hover:border-accent-cyan/30"
                          >
                            Next
                          </button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
