/**
 * OmniWatch — Dashboard Frontend
 * Component: ModelSettings Page
 * Phase: 3.3 (model-manager)
 * Purpose: LLM provider selection with two tabs (Ollama / External) —
 *   Ollama manages installed models via local daemon; External covers
 *   OpenRouter, Groq, and custom OpenAI-compatible endpoints.
 * Inputs: GET /config/model-settings, PUT /config/model-settings,
 *   POST /config/test-connection, GET /ollama/models,
 *   POST /ollama/pull, DELETE /ollama/models/{name}
 * Outputs: Saved model configuration + test connection results via ModelTestResult
 */

import { useState, useEffect, useCallback } from 'react'
import api from '../api/client'
import { ModelTestResult, type ModelTestResultProps } from '../components/ModelTestResult'

// ── Types ──────────────────────────────────────────────────────────

type ActiveTab = 'ollama' | 'external'
type ExternalProvider = 'openrouter' | 'groq' | 'custom'
type BackendProvider = 'ollama' | 'openrouter' | 'groq' | 'custom'

interface ModelSettingsData {
  provider: string
  api_key: string
  model_name: string
  base_url?: string
  temperature: number
  max_tokens: number
}

interface InstalledModel {
  name: string
  size: number
  modified_at: string
}

// ── Constants ──────────────────────────────────────────────────────

const DEFAULT_SETTINGS: ModelSettingsData = {
  provider: 'ollama',
  api_key: '',
  model_name: 'qwen3:8b',
  base_url: '',
  temperature: 0.7,
  max_tokens: 2048,
}

const OLLAMA_SUGGESTIONS = [
  'qwen3:8b',
  'llama3.2:3b',
  'codellama:7b',
  'minimax-m2.7:cloud',
  'gpt-oss:120b-cloud',
  'deepseek-v3.1:cloud',
]

const EXTERNAL_SUGGESTIONS: Record<ExternalProvider, string[]> = {
  openrouter: [
    'meta-llama/llama-3.2-3b-instruct',
    'mistralai/mistral-7b-instruct',
    'google/gemma-2-9b-it',
  ],
  groq: ['llama-3.2-3b-instant', 'mixtral-8x7b-32768', 'gemma2-9b-it'],
  custom: ['gpt-4o', 'claude-3-5-sonnet-20241022', 'gemini-2.0-flash'],
}

const EXTERNAL_PROVIDER_DEFAULTS: Record<ExternalProvider, string> = {
  openrouter: 'https://openrouter.ai/api/v1',
  groq: 'https://api.groq.com/openai/v1',
  custom: '',
}

const KNOWN_BASE_URLS = new Set([
  'https://openrouter.ai/api/v1',
  'https://api.groq.com/openai/v1',
])

// ── Helpers ────────────────────────────────────────────────────────

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  const val = bytes / Math.pow(1024, i)
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

function mapBackendTab(provider: string): { tab: ActiveTab; ext: ExternalProvider } {
  if (provider === 'openrouter' || provider === 'groq' || provider === 'custom') {
    return { tab: 'external', ext: provider as ExternalProvider }
  }
  return { tab: 'ollama', ext: 'openrouter' }
}

// ── Component ──────────────────────────────────────────────────────

export function ModelSettings() {
  // ── Tab state ───────────────────────────────────────────────────
  const [activeTab, setActiveTab] = useState<ActiveTab>('ollama')
  const [externalProvider, setExternalProvider] = useState<ExternalProvider>('openrouter')

  // ── Shared form state ───────────────────────────────────────────
  const [modelName, setModelName] = useState(DEFAULT_SETTINGS.model_name)
  const [apiKey, setApiKey] = useState(DEFAULT_SETTINGS.api_key)
  const [apiKeyTouched, setApiKeyTouched] = useState(false)
  const [baseUrl, setBaseUrl] = useState(DEFAULT_SETTINGS.base_url)
  const [temperature, setTemperature] = useState(DEFAULT_SETTINGS.temperature)
  const [maxTokens, setMaxTokens] = useState(DEFAULT_SETTINGS.max_tokens)

  // ── UI state ────────────────────────────────────────────────────
  const [showApiKey, setShowApiKey] = useState(false)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [toast, setToast] = useState<{ type: 'success' | 'error'; message: string } | null>(null)
  const [testResult, setTestResult] = useState<ModelTestResultProps | null>(null)
  const [showSuggestions, setShowSuggestions] = useState(false)

  // ── Ollama state ────────────────────────────────────────────────
  const [ollamaModels, setOllamaModels] = useState<InstalledModel[]>([])
  const [ollamaLoading, setOllamaLoading] = useState(false)
  const [pullName, setPullName] = useState('')
  const [pulling, setPulling] = useState(false)
  const [pullProgress, setPullProgress] = useState<{ status: string; completed?: number; total?: number; percent: number }>({ status: '', percent: 0 })
  const [deleting, setDeleting] = useState<string | null>(null)

  // ── Fetch installed Ollama models ───────────────────────────────

  const fetchOllamaModels = useCallback(async () => {
    setOllamaLoading(true)
    try {
      const { data } = await api.get<{ models: InstalledModel[] }>('/ollama/models')
      setOllamaModels(data.models ?? [])
    } catch {
      setOllamaModels([])
    } finally {
      setOllamaLoading(false)
    }
  }, [])

  // ── Load settings on mount ──────────────────────────────────────

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const { data } = await api.get<ModelSettingsData>('/config/model-settings')
        if (cancelled) return
        const { tab, ext } = mapBackendTab(data.provider ?? 'ollama')
        setActiveTab(tab)
        setExternalProvider(ext)
        setModelName(data.model_name ?? DEFAULT_SETTINGS.model_name)
        setApiKey(data.api_key ?? '')
        setBaseUrl(data.base_url ?? '')
        setTemperature(data.temperature ?? DEFAULT_SETTINGS.temperature)
        setMaxTokens(data.max_tokens ?? DEFAULT_SETTINGS.max_tokens)
      } catch {
        // Backend not reachable — keep defaults
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [])

  // ── Fetch Ollama models after loading settings ──────────────────

  useEffect(() => {
    if (!loading) fetchOllamaModels()
  }, [loading, fetchOllamaModels])

  // ── Toast auto-dismiss ──────────────────────────────────────────

  useEffect(() => {
    if (!toast) return
    const t = setTimeout(() => setToast(null), 4000)
    return () => clearTimeout(t)
  }, [toast])

  // ── External provider switch ────────────────────────────────────

  const handleExternalProviderSwitch = useCallback((newExt: ExternalProvider) => {
    setExternalProvider(newExt)
    // Auto-fill base_url if empty or matches a known default
    setBaseUrl((prev) => {
      if (!prev || KNOWN_BASE_URLS.has(prev)) {
        return EXTERNAL_PROVIDER_DEFAULTS[newExt]
      }
      return prev
    })
    // Reset to first suggestion for new provider
    const suggestions = EXTERNAL_SUGGESTIONS[newExt]
    if (suggestions.length > 0) {
      setModelName(suggestions[0])
    }
    setApiKeyTouched(false)
    setApiKey('')
    setTestResult(null)
  }, [])

  // ── Tab switch ──────────────────────────────────────────────────

  const handleTabSwitch = useCallback((tab: ActiveTab) => {
    setActiveTab(tab)
    setTestResult(null)
  }, [])

  // ── Pull model ──────────────────────────────────────────────────

  const handlePull = useCallback(async () => {
    const name = pullName.trim()
    if (!name) return
    setPulling(true)
    setPullProgress({ status: 'starting...', percent: 0 })
    setToast(null)
    try {
      const resp = await fetch('/api/ollama/pull', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      if (!resp.ok) {
        const text = await resp.text()
        throw new Error(text || `HTTP ${resp.status}`)
      }
      const contentType = resp.headers.get('content-type') || ''
      if (!contentType.includes('text/event-stream') || !resp.body) {
        // Non-streaming fallback (e.g. :cloud note)
        const data = await resp.json().catch(() => ({ success: true }))
        if (data.success === false) throw new Error(data.error || 'Pull failed')
        await fetchOllamaModels()
        setModelName(name)
        setPullName('')
        setPullProgress({ status: 'done', percent: 100 })
        setToast({ type: 'success', message: data.note || `Pulled ${name} successfully` })
        return
      }
      const reader = resp.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const parts = buffer.split('\n\n')
        buffer = parts.pop() || ''
        for (const part of parts) {
          const line = part.trim()
          if (!line.startsWith('data: ')) continue
          const payload = line.slice(6)
          try {
            const data = JSON.parse(payload)
            if (data.status === 'error') throw new Error(data.error || 'Pull failed')
            if (data.status === 'success') {
              setPullProgress({ status: 'verifying...', percent: 100 })
              continue
            }
            // Ollama progress: {status, digest, total, completed}
            const total: number | undefined = data.total
            const completed: number | undefined = data.completed
            const status: string = data.status || 'pulling'
            let percent = 0
            if (typeof total === 'number' && typeof completed === 'number' && total > 0) {
              percent = Math.min(100, Math.round((completed / total) * 100))
            } else if (status.includes('pulling manifest')) {
              percent = 1
            }
            setPullProgress({ status, total, completed, percent })
          } catch (e) {
            if (e instanceof Error && e.message !== 'Pull failed') throw e
          }
        }
      }
      await fetchOllamaModels()
      setModelName(name)
      setPullName('')
      setPullProgress({ status: 'done', percent: 100 })
      setToast({ type: 'success', message: `Pulled ${name} successfully` })
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Pull failed'
      setToast({ type: 'error', message: msg })
      setPullProgress({ status: 'error', percent: 0 })
    } finally {
      setPulling(false)
    }
  }, [pullName, fetchOllamaModels])

  // ── Delete model ────────────────────────────────────────────────

  const handleDelete = useCallback(async (name: string) => {
    if (!window.confirm(`Delete model "${name}"? This cannot be undone.`)) return
    setDeleting(name)
    setToast(null)
    try {
      await api.delete(`/ollama/models/${encodeURIComponent(name)}`)
      await fetchOllamaModels()
      // If deleted model was selected, clear modelName
      setModelName((prev) => (prev === name ? '' : prev))
      setToast({ type: 'success', message: `Deleted ${name}` })
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Delete failed'
      setToast({ type: 'error', message: msg })
    } finally {
      setDeleting(null)
    }
  }, [fetchOllamaModels])

  // ── Build model suggestions for current context ─────────────────

  const currentSuggestions = activeTab === 'ollama'
    ? [...ollamaModels.map((m) => m.name), ...OLLAMA_SUGGESTIONS.filter(
        (s) => !ollamaModels.some((m) => m.name === s),
      )]
    : EXTERNAL_SUGGESTIONS[externalProvider]

  // ── Active provider string for backend ───────────────────────────

  const backendProvider: BackendProvider = activeTab === 'ollama' ? 'ollama' : externalProvider

  // ── Test connection ─────────────────────────────────────────────

  const handleTestConnection = useCallback(async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const payload: Record<string, unknown> = {
        provider: backendProvider,
        model_name: modelName,
        temperature,
        max_tokens: maxTokens,
      }
      // Include api_key if touched — for Ollama :cloud this forwards Bearer auth;
      // absent-means-keep when untouched
      if (apiKeyTouched) {
        payload.api_key = apiKey
      }
      // Include base_url for external providers (always for custom, if edited for others)
      if (activeTab === 'external') {
        payload.base_url = baseUrl
      }
      const { data } = await api.post<{
        success: boolean
        provider: string
        model: string
        latency_ms: number
        error?: string
      }>('/config/test-connection', payload, { timeout: 240_000 })
      setTestResult({
        success: data.success,
        provider: data.provider,
        model: data.model,
        latencyMs: data.latency_ms,
        error: data.error,
      })
    } catch (err) {
      setTestResult({
        success: false,
        provider: backendProvider,
        model: modelName,
        latencyMs: 0,
        error: err instanceof Error ? err.message : 'Test request failed',
      })
    } finally {
      setTesting(false)
    }
  }, [backendProvider, modelName, temperature, maxTokens, apiKey, apiKeyTouched, activeTab, baseUrl])

  // ── Save settings ───────────────────────────────────────────────

  const handleSave = useCallback(async () => {
    setSaving(true)
    setToast(null)
    try {
      const payload: Record<string, unknown> = {
        provider: backendProvider,
        model_name: modelName,
        temperature,
        max_tokens: maxTokens,
      }
      // Include api_key if touched — Ollama :cloud needs Bearer forward; absent-means-keep when untouched
      if (apiKeyTouched) {
        payload.api_key = apiKey
      }
      // Include base_url for external providers
      if (activeTab === 'external') {
        payload.base_url = baseUrl
      }
      await api.put('/config/model-settings', payload)
      setToast({ type: 'success', message: 'Settings saved successfully' })
      if (apiKeyTouched) {
        setApiKeyTouched(false)
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Save failed'
      setToast({ type: 'error', message: msg })
    } finally {
      setSaving(false)
    }
  }, [backendProvider, modelName, temperature, maxTokens, apiKey, apiKeyTouched, activeTab, baseUrl])

  // ── Loading skeleton ────────────────────────────────────────────

  if (loading) {
    return (
      <div className="p-4 flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div>
            <div className="h-5 w-48 rounded bg-[#2a2a2a] animate-pulse" />
            <div className="h-3 w-72 rounded bg-[#2a2a2a] animate-pulse mt-2" />
          </div>
        </div>
        <div
          className="rounded-xl border border-[rgba(255,255,255,0.06)] p-6 space-y-5"
          style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}
        >
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-10 rounded-lg bg-[#2a2a2a] animate-pulse" />
          ))}
        </div>
      </div>
    )
  }

  // ── Render ──────────────────────────────────────────────────────

  return (
    <div className="p-4 flex flex-col gap-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1
            className="font-heading text-lg text-[#e4e4e7]"
            style={{ fontFamily: "'Space Grotesk', sans-serif" }}
          >
            Model Settings
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">
            Configure LLM provider, model, and connection parameters
          </p>
        </div>
      </div>

      {/* Toast */}
      {toast && (
        <div
          className={
            'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-mono transition-all duration-200 ' +
            (toast.type === 'success'
              ? 'bg-[rgba(34,197,94,0.12)] border border-[rgba(34,197,94,0.3)] text-[#22c55e]'
              : 'bg-[rgba(239,68,68,0.12)] border border-[rgba(239,68,68,0.3)] text-[#ef4444]')
          }
        >
          <span>{toast.type === 'success' ? '✓' : '✕'}</span>
          <span>{toast.message}</span>
        </div>
      )}

      {/* ── Tab Bar ──────────────────────────────────────────────── */}
      <div className="flex gap-1 p-1 rounded-lg bg-[#111113] border border-[rgba(255,255,255,0.06)]">
        <button
          type="button"
          onClick={() => handleTabSwitch('ollama')}
          className={
            'flex-1 px-4 py-2.5 rounded-md text-sm font-mono font-medium transition-all duration-150 ' +
            (activeTab === 'ollama'
              ? 'bg-[rgba(0,212,255,0.08)] border border-[rgba(0,212,255,0.4)] text-[#00d4ff] shadow-[0_0_12px_rgba(0,212,255,0.06)]'
              : 'border border-transparent text-[#a1a1aa] hover:text-[#e2e2e5] hover:bg-[rgba(255,255,255,0.02)]')
          }
        >
          Ollama
        </button>
        <button
          type="button"
          onClick={() => handleTabSwitch('external')}
          className={
            'flex-1 px-4 py-2.5 rounded-md text-sm font-mono font-medium transition-all duration-150 ' +
            (activeTab === 'external'
              ? 'bg-[rgba(0,212,255,0.08)] border border-[rgba(0,212,255,0.4)] text-[#00d4ff] shadow-[0_0_12px_rgba(0,212,255,0.06)]'
              : 'border border-transparent text-[#a1a1aa] hover:text-[#e2e2e5] hover:bg-[rgba(255,255,255,0.02)]')
          }
        >
          External Model
        </button>
      </div>

      {/* ═══════════════════════════════════════════════════════════════
          TAB 1: OLLAMA
          ═══════════════════════════════════════════════════════════════ */}
      {activeTab === 'ollama' && (
        <div className="flex flex-col gap-4">
          {/* Info banner */}
          <div className="px-4 py-3 rounded-xl border border-[rgba(0,212,255,0.15)] bg-[rgba(0,212,255,0.04)] text-[#a1a1aa] text-xs font-mono leading-relaxed">
            Local models (e.g. <span className="text-[#e2e2e5]">qwen3:8b</span>) run offline. Cloud models (e.g.&nbsp;
            <span className="text-[#00d4ff]">minimax-m2.7:cloud</span>) run on Ollama Cloud — just paste your key from&nbsp;
            <a href="https://ollama.com/settings/keys" target="_blank" rel="noreferrer" className="text-[#00d4ff] underline">ollama.com/settings/keys</a>&nbsp;
            into the <span className="text-[#e2e2e5]">Ollama API Key</span> field below and Save.
          </div>

          {/* ── Model Management ───────────────────────────────────── */}
          <div
            className="rounded-xl border border-[rgba(255,255,255,0.06)] p-6 flex flex-col gap-5 transition-all duration-200 hover:border-[rgba(0,212,255,0.15)]"
            style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}
          >
            {/* Installed models list */}
            <fieldset className="flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <legend className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]">
                  Installed Models
                </legend>
                <button
                  type="button"
                  onClick={fetchOllamaModels}
                  disabled={ollamaLoading}
                  className="text-[10px] font-mono text-[#00d4ff] hover:text-[#00b8db] transition-colors disabled:opacity-50"
                >
                  {ollamaLoading ? 'Refreshing...' : 'Refresh'}
                </button>
              </div>

              {ollamaLoading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => (
                    <div key={i} className="h-10 rounded-lg bg-[#2a2a2a] animate-pulse" />
                  ))}
                </div>
              ) : ollamaModels.length === 0 ? (
                <div className="px-3 py-4 rounded-lg border border-[rgba(255,255,255,0.04)] bg-[rgba(0,0,0,0.15)] text-[#52525b] text-xs font-mono text-center">
                  No models pulled yet
                </div>
              ) : (
                <div className="flex flex-col gap-1">
                  {ollamaModels.map((m) => (
                    <div
                      key={m.name}
                      className={
                        'flex items-center justify-between px-3 py-2.5 rounded-lg border transition-all duration-150 ' +
                        (modelName === m.name
                          ? 'border-[rgba(0,212,255,0.3)] bg-[rgba(0,212,255,0.06)]'
                          : 'border-[rgba(255,255,255,0.04)] bg-[rgba(0,0,0,0.15)] hover:border-[rgba(255,255,255,0.08)]')
                      }
                    >
                      <div className="flex flex-col gap-0.5 min-w-0">
                        <span className="text-sm font-mono text-[#e2e2e5] truncate">{m.name}</span>
                        <span className="text-[10px] font-mono text-[#52525b]">
                          {formatBytes(m.size)} · {new Date(m.modified_at).toLocaleDateString()}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 shrink-0 ml-3">
                        <button
                          type="button"
                          onClick={() => setModelName(m.name)}
                          className={
                            'px-2.5 py-1 rounded-md text-[11px] font-mono transition-all duration-150 ' +
                            (modelName === m.name
                              ? 'bg-[rgba(0,212,255,0.15)] text-[#00d4ff] border border-[rgba(0,212,255,0.3)]'
                              : 'border border-[rgba(255,255,255,0.08)] text-[#a1a1aa] hover:text-[#e2e2e5] hover:border-[rgba(255,255,255,0.15)]')
                          }
                        >
                          {modelName === m.name ? 'Selected' : 'Select'}
                        </button>
                        <button
                          type="button"
                          onClick={() => handleDelete(m.name)}
                          disabled={deleting === m.name}
                          className="px-2.5 py-1 rounded-md text-[11px] font-mono border border-[rgba(239,68,68,0.2)] text-[#ef4444] hover:bg-[rgba(239,68,68,0.08)] transition-all duration-150 disabled:opacity-50"
                        >
                          {deleting === m.name ? '...' : 'Delete'}
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </fieldset>

            {/* Pull section */}
            <div className="flex flex-col gap-2">
              <label className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]">
                Pull Model
              </label>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={pullName}
                  onChange={(e) => setPullName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handlePull()}
                  placeholder="e.g. qwen3:8b or minimax-m2.7:cloud"
                  className="flex-1 px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
                />
                <button
                  type="button"
                  onClick={handlePull}
                  disabled={pulling || !pullName.trim()}
                  className={
                    'px-4 py-2.5 rounded-lg border text-sm font-mono transition-all duration-150 ' +
                    (pulling
                      ? 'border-[rgba(255,255,255,0.06)] bg-[rgba(255,255,255,0.02)] text-[#52525b] cursor-not-allowed'
                      : 'border-[rgba(0,212,255,0.3)] bg-[rgba(0,212,255,0.06)] text-[#00d4ff] hover:bg-[rgba(0,212,255,0.12)] hover:border-[rgba(0,212,255,0.5)]')
                  }
                >
                  {pulling ? (
                    <span className="flex items-center gap-2">
                      <span className="w-3.5 h-3.5 rounded-full border-2 border-[#00d4ff] border-t-transparent animate-spin" />
                      Pulling...
                    </span>
                  ) : (
                    'Pull'
                  )}
                </button>
              </div>
              {pulling && (
                <div className="flex flex-col gap-1.5 px-1">
                  <div className="h-2 rounded-full bg-[#1a1a1a] border border-[rgba(255,255,255,0.06)] overflow-hidden">
                    <div
                      className="h-full bg-[#00d4ff] transition-all duration-300"
                      style={{ width: `${pullProgress.percent}%` }}
                    />
                  </div>
                  <div className="flex justify-between text-[10px] font-mono">
                    <span className="text-[#a1a1aa] truncate">
                      {pullProgress.status || 'downloading...'}
                      {typeof pullProgress.completed === 'number' && typeof pullProgress.total === 'number'
                        ? ` — ${formatBytes(pullProgress.completed)} / ${formatBytes(pullProgress.total)}`
                        : ''}
                    </span>
                    <span className="text-[#00d4ff] shrink-0 ml-2">{pullProgress.percent}%</span>
                  </div>
                </div>
              )}
            </div>

            {/* Model name with suggestions */}
            <div className="flex flex-col gap-2 relative">
              <label
                htmlFor="ollama-model-name"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Model Name
              </label>
              <input
                id="ollama-model-name"
                type="text"
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                onFocus={() => setShowSuggestions(true)}
                onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                placeholder="e.g. qwen3:8b"
                className="w-full px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
              />
              {showSuggestions && currentSuggestions.length > 0 && (
                <div className="absolute top-full left-0 right-0 z-10 mt-1 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[#1a1a1a] shadow-lg overflow-hidden max-h-48 overflow-y-auto">
                  {currentSuggestions.map((suggestion) => (
                    <button
                      key={suggestion}
                      type="button"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => {
                        setModelName(suggestion)
                        setShowSuggestions(false)
                      }}
                      className={
                        'w-full text-left px-3 py-2 text-sm font-mono transition-colors ' +
                        (modelName === suggestion
                          ? 'bg-[rgba(0,212,255,0.08)] text-[#00d4ff]'
                          : 'text-[#a1a1aa] hover:bg-[rgba(255,255,255,0.04)] hover:text-[#e2e2e5]')
                      }
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Ollama Cloud API Key — only needed for :cloud models */}
            <div className="flex flex-col gap-2">
              <label
                htmlFor="ollama-api-key"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Ollama API Key <span className="text-[#52525b] normal-case tracking-normal">(optional — only for :cloud models)</span>
              </label>
              <div className="relative">
                <input
                  id="ollama-api-key"
                  type={showApiKey ? 'text' : 'password'}
                  value={apiKey}
                  onChange={(e) => {
                    setApiKey(e.target.value)
                    setApiKeyTouched(true)
                  }}
                  placeholder={modelName.endsWith(':cloud') || modelName.includes(':cloud') ? 'Paste key from ollama.com/settings/keys or leave empty if signed in' : 'Leave empty for local models — needed only for :cloud'}
                  autoComplete="off"
                  className="w-full px-3 py-2.5 pr-10 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
                />
                <button
                  type="button"
                  onClick={() => setShowApiKey(!showApiKey)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-[#a1a1aa] hover:text-[#e2e2e5] transition-colors text-xs"
                  tabIndex={-1}
                >
                  {showApiKey ? 'Hide' : 'Show'}
                </button>
              </div>
              <span className="text-[10px] font-mono text-[#52525b] leading-relaxed">
                For <span className="text-[#00d4ff]">:cloud</span> models run <span className="text-[#e2e2e5]">docker exec -it omniwatch-ollama ollama signin</span> once, or paste your key here. Local models (e.g. <span className="text-[#e2e2e5]">qwen3:8b</span>) don&apos;t need it.
              </span>
            </div>

            {/* Temperature */}
            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <label
                  htmlFor="ollama-temperature"
                  className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
                >
                  Temperature
                </label>
                <span className="text-xs font-mono text-[#00d4ff]">{temperature.toFixed(1)}</span>
              </div>
              <input
                id="ollama-temperature"
                type="range"
                min="0"
                max="2"
                step="0.1"
                value={temperature}
                onChange={(e) => setTemperature(parseFloat(e.target.value))}
                className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-[#00d4ff]"
                style={{
                  background: `linear-gradient(to right, #00d4ff ${(temperature / 2) * 100}%, #2a2a2a ${(temperature / 2) * 100}%)`,
                }}
              />
              <div className="flex justify-between text-[10px] font-mono text-[#52525b]">
                <span>0.0</span>
                <span>1.0</span>
                <span>2.0</span>
              </div>
            </div>

            {/* Max Tokens */}
            <div className="flex flex-col gap-2">
              <label
                htmlFor="ollama-max-tokens"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Max Tokens
              </label>
              <input
                id="ollama-max-tokens"
                type="number"
                min={256}
                max={8192}
                step={64}
                value={maxTokens}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10)
                  if (!isNaN(v)) setMaxTokens(v)
                }}
                className="w-full px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
              />
              <span className="text-[10px] font-mono text-[#52525b]">Range: 256 — 8192</span>
            </div>
          </div>

          {/* Action buttons */}
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={handleTestConnection}
              disabled={testing || !modelName.trim()}
              className={
                'px-4 py-2.5 rounded-lg border text-sm font-mono transition-all duration-150 ' +
                (testing || !modelName.trim()
                  ? 'border-[rgba(255,255,255,0.06)] bg-[rgba(255,255,255,0.02)] text-[#52525b] cursor-not-allowed'
                  : 'border-[rgba(0,212,255,0.3)] bg-[rgba(0,212,255,0.06)] text-[#00d4ff] hover:bg-[rgba(0,212,255,0.12)] hover:border-[rgba(0,212,255,0.5)]')
              }
            >
              {testing ? (
                <span className="flex items-center gap-2">
                  <span className="w-3.5 h-3.5 rounded-full border-2 border-[#00d4ff] border-t-transparent animate-spin" />
                  Testing...
                </span>
              ) : (
                'Test Connection'
              )}
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving || !modelName.trim()}
              className={
                'px-5 py-2.5 rounded-lg text-sm font-mono font-medium transition-all duration-150 ' +
                (saving || !modelName.trim()
                  ? 'bg-[#2a2a2a] text-[#52525b] cursor-not-allowed'
                  : 'bg-[#00d4ff] text-black hover:bg-[#00b8db] hover:shadow-[0_0_15px_rgba(0,212,255,0.15)]')
              }
            >
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════
          TAB 2: EXTERNAL MODEL
          ═══════════════════════════════════════════════════════════════ */}
      {activeTab === 'external' && (
        <div className="flex flex-col gap-4">
          {/* Main settings card */}
          <div
            className="rounded-xl border border-[rgba(255,255,255,0.06)] p-6 flex flex-col gap-5 transition-all duration-200 hover:border-[rgba(0,212,255,0.15)]"
            style={{ background: 'linear-gradient(135deg, #1a1a1a, #141618)' }}
          >
            {/* Provider selector */}
            <fieldset className="flex flex-col gap-2">
              <legend className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa] mb-1">
                External Provider
              </legend>
              <div className="grid grid-cols-3 gap-2">
                {(['openrouter', 'groq', 'custom'] as const).map((p) => (
                  <button
                    key={p}
                    type="button"
                    onClick={() => handleExternalProviderSwitch(p)}
                    className={
                      'px-3 py-2.5 rounded-lg border text-sm font-mono capitalize transition-all duration-150 ' +
                      (externalProvider === p
                        ? 'border-[rgba(0,212,255,0.4)] bg-[rgba(0,212,255,0.08)] text-[#00d4ff] shadow-[0_0_12px_rgba(0,212,255,0.06)]'
                        : 'border-[rgba(255,255,255,0.06)] bg-[rgba(255,255,255,0.02)] text-[#a1a1aa] hover:border-[rgba(255,255,255,0.12)] hover:text-[#e2e2e5]')
                    }
                  >
                    {p === 'openrouter' ? 'OpenRouter' : p === 'groq' ? 'Groq' : 'Custom'}
                  </button>
                ))}
              </div>
            </fieldset>

            {/* API Key */}
            <div className="flex flex-col gap-2">
              <label
                htmlFor="ext-api-key"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                API Key
              </label>
              <div className="relative">
                <input
                  id="ext-api-key"
                  type={showApiKey ? 'text' : 'password'}
                  value={apiKey}
                  onChange={(e) => {
                    setApiKey(e.target.value)
                    setApiKeyTouched(true)
                  }}
                  placeholder="Enter your API key..."
                  autoComplete="off"
                  className="w-full px-3 py-2.5 pr-10 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
                />
                <button
                  type="button"
                  onClick={() => setShowApiKey(!showApiKey)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-[#a1a1aa] hover:text-[#e2e2e5] transition-colors text-xs"
                  tabIndex={-1}
                >
                  {showApiKey ? 'Hide' : 'Show'}
                </button>
              </div>
            </div>

            {/* Base URL */}
            <div className="flex flex-col gap-2">
              <label
                htmlFor="ext-base-url"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Base URL
              </label>
              <input
                id="ext-base-url"
                type="text"
                value={baseUrl}
                onChange={(e) => setBaseUrl(e.target.value)}
                placeholder={
                  externalProvider === 'custom'
                    ? 'https://api.openai.com/v1 or https://api.together.xyz/v1'
                    : EXTERNAL_PROVIDER_DEFAULTS[externalProvider]
                }
                className="w-full px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
              />
              <span className="text-[10px] font-mono text-[#52525b]">
                {externalProvider === 'custom'
                  ? 'Required for custom providers'
                  : 'Pre-filled — edit only if using a proxy or mirror'}
              </span>
            </div>

            {/* Model name with suggestions */}
            <div className="flex flex-col gap-2 relative">
              <label
                htmlFor="ext-model-name"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Model Name
              </label>
              <input
                id="ext-model-name"
                type="text"
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                onFocus={() => setShowSuggestions(true)}
                onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                placeholder={
                  externalProvider === 'openrouter'
                    ? 'e.g. meta-llama/llama-3.2-3b-instruct'
                    : externalProvider === 'groq'
                      ? 'e.g. llama-3.2-3b-instant'
                      : 'e.g. gpt-4o'
                }
                className="w-full px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
              />
              {showSuggestions && currentSuggestions.length > 0 && (
                <div className="absolute top-full left-0 right-0 z-10 mt-1 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[#1a1a1a] shadow-lg overflow-hidden">
                  {currentSuggestions.map((suggestion) => (
                    <button
                      key={suggestion}
                      type="button"
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => {
                        setModelName(suggestion)
                        setShowSuggestions(false)
                      }}
                      className={
                        'w-full text-left px-3 py-2 text-sm font-mono transition-colors ' +
                        (modelName === suggestion
                          ? 'bg-[rgba(0,212,255,0.08)] text-[#00d4ff]'
                          : 'text-[#a1a1aa] hover:bg-[rgba(255,255,255,0.04)] hover:text-[#e2e2e5]')
                      }
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Temperature */}
            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <label
                  htmlFor="ext-temperature"
                  className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
                >
                  Temperature
                </label>
                <span className="text-xs font-mono text-[#00d4ff]">{temperature.toFixed(1)}</span>
              </div>
              <input
                id="ext-temperature"
                type="range"
                min="0"
                max="2"
                step="0.1"
                value={temperature}
                onChange={(e) => setTemperature(parseFloat(e.target.value))}
                className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-[#00d4ff]"
                style={{
                  background: `linear-gradient(to right, #00d4ff ${(temperature / 2) * 100}%, #2a2a2a ${(temperature / 2) * 100}%)`,
                }}
              />
              <div className="flex justify-between text-[10px] font-mono text-[#52525b]">
                <span>0.0</span>
                <span>1.0</span>
                <span>2.0</span>
              </div>
            </div>

            {/* Max Tokens */}
            <div className="flex flex-col gap-2">
              <label
                htmlFor="ext-max-tokens"
                className="text-[10px] uppercase tracking-widest font-mono text-[#a1a1aa]"
              >
                Max Tokens
              </label>
              <input
                id="ext-max-tokens"
                type="number"
                min={256}
                max={8192}
                step={64}
                value={maxTokens}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10)
                  if (!isNaN(v)) setMaxTokens(v)
                }}
                className="w-full px-3 py-2.5 rounded-lg border border-[rgba(255,255,255,0.08)] bg-[rgba(0,0,0,0.2)] text-[#e2e2e5] text-sm font-mono placeholder-[#52525b] outline-none transition-all duration-150 focus:border-[rgba(0,212,255,0.3)] focus:shadow-[0_0_8px_rgba(0,212,255,0.06)]"
              />
              <span className="text-[10px] font-mono text-[#52525b]">Range: 256 — 8192</span>
            </div>
          </div>

          {/* Action buttons */}
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={handleTestConnection}
              disabled={testing || !modelName.trim() || !apiKey.trim()}
              className={
                'px-4 py-2.5 rounded-lg border text-sm font-mono transition-all duration-150 ' +
                (testing || !modelName.trim() || !apiKey.trim()
                  ? 'border-[rgba(255,255,255,0.06)] bg-[rgba(255,255,255,0.02)] text-[#52525b] cursor-not-allowed'
                  : 'border-[rgba(0,212,255,0.3)] bg-[rgba(0,212,255,0.06)] text-[#00d4ff] hover:bg-[rgba(0,212,255,0.12)] hover:border-[rgba(0,212,255,0.5)]')
              }
            >
              {testing ? (
                <span className="flex items-center gap-2">
                  <span className="w-3.5 h-3.5 rounded-full border-2 border-[#00d4ff] border-t-transparent animate-spin" />
                  Testing...
                </span>
              ) : (
                'Test Connection'
              )}
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving || !modelName.trim() || !apiKey.trim()}
              className={
                'px-5 py-2.5 rounded-lg text-sm font-mono font-medium transition-all duration-150 ' +
                (saving || !modelName.trim() || !apiKey.trim()
                  ? 'bg-[#2a2a2a] text-[#52525b] cursor-not-allowed'
                  : 'bg-[#00d4ff] text-black hover:bg-[#00b8db] hover:shadow-[0_0_15px_rgba(0,212,255,0.15)]')
              }
            >
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </div>
      )}

      {/* ── Test Result ──────────────────────────────────────────── */}
      {testResult && (
        <ModelTestResult
          success={testResult.success}
          provider={testResult.provider}
          model={testResult.model}
          latencyMs={testResult.latencyMs}
          error={testResult.error}
        />
      )}
    </div>
  )
}

export default ModelSettings
