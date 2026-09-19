import { useState, useCallback, useRef } from 'react'
import { fetchMinioBuckets, fetchMinioObjects, minioUpload, minioDownload, minioDeleteObject, minioDeleteBucket, minioCreateBucket, apiError } from '../api/client'
import type { MinioBucket, MinioObject } from '../api/client'
import { AuthGate } from '../components/AuthGate'

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

  const [uploading, setUploading] = useState(false)
  const [showCreateBucket, setShowCreateBucket] = useState(false)
  const [newBucketName, setNewBucketName] = useState('')
  const [actionMsg, setActionMsg] = useState<string | null>(null)
  const [actionErr, setActionErr] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [previewName, setPreviewName] = useState<string | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const [previewText, setPreviewText] = useState<string | null>(null)
  const [previewKind, setPreviewKind] = useState<'image' | 'text' | 'pdf' | 'other'>('other')
  const [previewLoading, setPreviewLoading] = useState(false)

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
      setBucketsError(apiError(e, 'Failed to load buckets'))
      setBuckets([])
    } finally {
      setBucketsLoading(false)
    }
  }, [])

  const loadObjects = useCallback(async (bucket: string, pfx: string, off: number) => {
    setObjectsLoading(true)
    setObjectsError(null)
    try {
      const res = await fetchMinioObjects({ bucket, prefix: pfx, limit, offset: off })
      if (res.error) {
        setObjectsError(res.error)
        setObjects(res.objects ?? [])
        setTotal(res.total ?? 0)
      } else {
        setObjects(res.objects)
        setTotal(res.total)
      }
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

  const handleUpload = async (file: File) => {
    if (!selectedBucket) return
    setUploading(true)
    setActionMsg(null)
    setActionErr(null)
    try {
      await minioUpload(selectedBucket, file.name, file)
      setActionMsg(`Uploaded ${file.name}`)
      loadObjects(selectedBucket, prefix, offset)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const handlePreview = async (objName: string) => {
    if (!selectedBucket) return
    setPreviewLoading(true)
    setActionErr(null)
    try {
      const blob = await minioDownload(selectedBucket, objName)
      const ext = objName.split('.').pop()?.toLowerCase() ?? ''
      const mime = blob.type || ''
      const isImage = mime.startsWith('image/') || ['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp'].includes(ext)
      const isPdf = mime === 'application/pdf' || ext === 'pdf'
      const isText = mime.startsWith('text/') || mime.includes('json') || ['txt', 'log', 'json', 'csv', 'md', 'xml', 'yaml', 'yml', 'js', 'ts', 'tsx', 'py', 'html', 'css', 'conf', 'env'].includes(ext)
      if (previewUrl) URL.revokeObjectURL(previewUrl)
      if (isImage) {
        setPreviewUrl(URL.createObjectURL(blob))
        setPreviewText(null)
        setPreviewKind('image')
      } else if (isPdf) {
        setPreviewUrl(URL.createObjectURL(blob))
        setPreviewText(null)
        setPreviewKind('pdf')
      } else if (isText) {
        const text = await blob.text()
        setPreviewText(text.slice(0, 20000))
        setPreviewUrl(null)
        setPreviewKind('text')
      } else {
        setPreviewUrl(URL.createObjectURL(blob))
        setPreviewText(null)
        setPreviewKind('other')
      }
      setPreviewName(objName)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Preview failed')
    } finally {
      setPreviewLoading(false)
    }
  }

  const closePreview = () => {
    if (previewUrl) URL.revokeObjectURL(previewUrl)
    setPreviewUrl(null)
    setPreviewText(null)
    setPreviewName(null)
  }

  const handleDownload = async (objName: string) => {
    if (!selectedBucket) return
    try {
      const blob = await minioDownload(selectedBucket, objName)
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = objName.split('/').pop() || objName
      a.click()
      URL.revokeObjectURL(url)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Download failed')
    }
  }

  const handleDeleteObject = async (objName: string) => {
    if (!selectedBucket || !confirm(`Delete ${objName}?`)) return
    setActionMsg(null)
    setActionErr(null)
    try {
      await minioDeleteObject(selectedBucket, objName)
      setActionMsg(`Deleted ${objName}`)
      loadObjects(selectedBucket, prefix, offset)
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Delete failed')
    }
  }

  const handleCreateBucket = async () => {
    if (!newBucketName.trim()) return
    setActionMsg(null)
    setActionErr(null)
    try {
      await minioCreateBucket(newBucketName.trim())
      setActionMsg(`Created bucket ${newBucketName.trim()}`)
      setNewBucketName('')
      setShowCreateBucket(false)
      loadBuckets()
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Create bucket failed')
    }
  }

  const handleDeleteBucket = async (name: string) => {
    if (!confirm(`Delete bucket "${name}"? This only works if the bucket is empty.`)) return
    setActionMsg(null)
    setActionErr(null)
    try {
      await minioDeleteBucket(name)
      setActionMsg(`Deleted bucket ${name}`)
      if (selectedBucket === name) {
        setSelectedBucket(null)
        setObjects([])
        setTotal(0)
      }
      loadBuckets()
    } catch (e) {
      setActionErr(e instanceof Error ? e.message : 'Delete bucket failed')
    }
  }

  const totalPages = Math.ceil(total / limit)
  const currentPage = Math.floor(offset / limit) + 1

  return (
    <AuthGate service="minio" onAuth={loadBuckets}>
      <div className="p-4 flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-text-primary">MinIO Browser</h1>
          <p className="text-xs text-text-muted font-mono">Live buckets & objects via MinIO SDK — GET /api/minio/buckets · /api/minio/objects?bucket=</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowCreateBucket(!showCreateBucket)}
            className="px-3 py-1.5 text-xs font-mono rounded bg-accent-cyan/10 hover:bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/30"
          >
            + Bucket
          </button>
          <button
            onClick={() => { if (selectedBucket) { loadBuckets(); loadObjects(selectedBucket, prefix, offset) } else loadBuckets() }}
            className="px-3 py-1.5 text-xs font-mono rounded border border-[#2a2a2a] hover:border-accent-cyan/40 hover:text-accent-cyan transition-colors"
          >
            Refresh
          </button>
        </div>
      </div>

      {actionMsg && (
        <div className="px-3 py-2 text-xs font-mono rounded bg-green-900/20 border border-green-900/40 text-green-400 flex items-center justify-between">
          {actionMsg}
          <button onClick={() => setActionMsg(null)} className="text-green-400/60 hover:text-green-400">×</button>
        </div>
      )}
      {actionErr && (
        <div className="px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400 flex items-center justify-between">
          {actionErr}
          <button onClick={() => setActionErr(null)} className="text-red-400/60 hover:text-red-400">×</button>
        </div>
      )}

      {showCreateBucket && (
        <div className="card p-3 rounded-lg border border-[#2a2a2a] flex items-center gap-2">
          <input
            value={newBucketName}
            onChange={(e) => setNewBucketName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleCreateBucket()}
            placeholder="new-bucket-name"
            className="px-2 py-1 text-xs bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono flex-1 focus:outline-none focus:border-accent-cyan/40"
          />
          <button onClick={handleCreateBucket} className="px-3 py-1 text-xs rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30">Create</button>
          <button onClick={() => { setShowCreateBucket(false); setNewBucketName('') }} className="px-3 py-1 text-xs rounded border border-[#2a2a2a] hover:border-red-900/40 text-text-muted">Cancel</button>
        </div>
      )}

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
                <div key={b.name} className={`flex items-center group ${selectedBucket === b.name ? 'bg-accent-cyan/10 border-l-2 border-accent-cyan' : 'border-l-2 border-transparent'}`}>
                  <button
                    onClick={() => handleSelectBucket(b.name)}
                    className="flex-1 text-left px-3 py-2.5 text-sm flex flex-col gap-0.5 hover:bg-[#1a1a1a] transition-colors"
                  >
                    <span className={`font-mono text-xs truncate ${selectedBucket === b.name ? 'text-accent-cyan' : 'text-text-primary'}`}>{b.name}</span>
                    {b.creation_date && <span className="text-[10px] text-text-muted">{new Date(b.creation_date).toLocaleString()}</span>}
                  </button>
                  <button
                    onClick={() => handleDeleteBucket(b.name)}
                    className="px-2 py-1 mr-2 text-[10px] font-mono rounded opacity-0 group-hover:opacity-100 hover:bg-red-900/20 text-red-400/50 hover:text-red-400 transition-all"
                    title="Delete bucket"
                  >
                    ×
                  </button>
                </div>
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
                    <button
                      onClick={() => fileInputRef.current?.click()}
                      disabled={uploading}
                      className="px-2 py-1 text-xs rounded bg-accent-cyan/10 hover:bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/30 disabled:opacity-50"
                    >
                      {uploading ? 'Uploading...' : 'Upload'}
                    </button>
                    <input
                      ref={fileInputRef}
                      type="file"
                      className="hidden"
                      onChange={(e) => { const f = e.target.files?.[0]; if (f) handleUpload(f); e.target.value = '' }}
                    />
                    <button onClick={handleRefresh} className="px-2 py-1 text-xs rounded border border-[#2a2a2a] hover:border-accent-cyan/30">↻</button>
                    <button
                      onClick={() => { if (selectedBucket && confirm(`Delete bucket "${selectedBucket}"? Only works if empty.`)) handleDeleteBucket(selectedBucket) }}
                      className="px-2 py-1 text-xs rounded border border-red-900/30 hover:bg-red-900/20 text-red-400/70 hover:text-red-400"
                    >
                      Delete Bucket
                    </button>
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
                            <th className="text-right px-3 py-2 font-normal">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-[#1e1e1e]">
                          {objects.map((o) => (
                            <tr key={o.name} className="hover:bg-[#141618]">
                              <td className="px-3 py-1.5 font-mono truncate max-w-[300px]" title={o.name}>{o.name}</td>
                              <td className="px-3 py-1.5 text-right font-mono text-text-muted">{formatSize(o.size)}</td>
                              <td className="px-3 py-1.5 font-mono text-text-muted">{o.last_modified ? new Date(o.last_modified).toLocaleString() : '—'}</td>
                              <td className="px-3 py-1.5 text-right">
                                <div className="flex items-center justify-end gap-1">
                                  <button
                                    onClick={() => handlePreview(o.name)}
                                    className="px-1.5 py-0.5 text-[10px] font-mono rounded hover:bg-accent-cyan/10 text-accent-cyan/60 hover:text-accent-cyan transition-colors"
                                    title="Preview"
                                  >
                                    👁
                                  </button>
                                  <button
                                    onClick={() => handleDownload(o.name)}
                                    className="px-1.5 py-0.5 text-[10px] font-mono rounded hover:bg-accent-cyan/10 text-accent-cyan/60 hover:text-accent-cyan transition-colors"
                                    title="Download"
                                  >
                                    ↓
                                  </button>
                                  <button
                                    onClick={() => handleDeleteObject(o.name)}
                                    className="px-1.5 py-0.5 text-[10px] font-mono rounded hover:bg-red-900/20 text-red-400/50 hover:text-red-400 transition-colors"
                                    title="Delete"
                                  >
                                    ×
                                  </button>
                                </div>
                              </td>
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
      {previewLoading && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="text-sm text-text-muted font-mono animate-pulse">Loading preview...</div>
        </div>
      )}
      {previewName && !previewLoading && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={closePreview}>
          <div className="card rounded-lg border border-[#2a2a2a] bg-[#0a0a0f] max-w-3xl w-full max-h-[80vh] flex flex-col overflow-hidden" onClick={(e) => e.stopPropagation()}>
            <div className="px-3 py-2 flex items-center gap-2 border-b border-[#2a2a2a]">
              <span className="text-xs font-mono text-text-primary truncate flex-1" title={previewName}>{previewName}</span>
              <button
                onClick={() => { if (selectedBucket && previewName) handleDownload(previewName) }}
                className="px-2 py-1 text-[10px] font-mono rounded bg-accent-cyan/10 hover:bg-accent-cyan/20 text-accent-cyan border border-accent-cyan/30"
              >
                Download
              </button>
              <button onClick={closePreview} className="px-2 py-1 text-xs rounded border border-[#2a2a2a] hover:border-red-900/40 text-text-muted">✕</button>
            </div>
            <div className="overflow-auto p-3 flex-1">
              {previewKind === 'image' && previewUrl ? (
                <img src={previewUrl} alt={previewName} className="max-w-full max-h-[60vh] mx-auto rounded" />
              ) : previewKind === 'pdf' && previewUrl ? (
                <iframe src={previewUrl} title={previewName} className="w-full h-[60vh] rounded bg-white" />
              ) : previewKind === 'text' && previewText !== null ? (
                <pre className="text-xs font-mono whitespace-pre-wrap break-words text-text-primary">{previewText}</pre>
              ) : (
                <div className="text-center py-8">
                  <div className="text-sm text-text-muted">No inline preview for this file type.</div>
                  <div className="text-xs text-text-muted/70 mt-1 font-mono">Use Download to view it locally.</div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
    </AuthGate>
  )
}
