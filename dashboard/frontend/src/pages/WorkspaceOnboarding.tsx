import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  APP_TYPES,
  CLOUD_PROVIDERS,
  VOLUME_BANDS,
  apiError,
  getOnboarding,
  getWorkspace,
  submitOnboarding,
  type AppType,
  type CloudProvider,
  type OnboardingAnswers,
  type OnboardingResult,
  type VolumeBand,
} from '../api/client'

const inputClass =
  'w-full px-3 py-2 text-sm bg-[#0a0a0f] border border-[#2a2a2a] rounded font-mono text-text-primary focus:outline-none focus:border-accent-cyan/60'
const btnPrimary =
  'px-4 py-1.5 text-xs font-mono rounded bg-accent-cyan/20 hover:bg-accent-cyan/30 text-accent-cyan border border-accent-cyan/30 disabled:opacity-40'
const btnGhost =
  'px-4 py-1.5 text-xs font-mono rounded text-text-muted hover:text-text-primary hover:bg-[#1a1a1a] disabled:opacity-40'

const STEPS = ['Application', 'Infrastructure', 'Capacity', 'Review'] as const

// Client-side mirror of identity/models.py EXEC_MARKERS (subset of the
// cheapest structural signals). The backend is authoritative — every submit
// still goes through server-side 422 — this only gives instant feedback.
const CLIENT_EXEC_MARKERS = [
  '<script', '</script', '<iframe', 'javascript:', '${{', '$(', '`',
  '&&', '||', 'exec(', 'system(', 'drop table', 'delete from',
]

function containsExecMarker(value: string): boolean {
  const lowered = value.toLowerCase()
  return CLIENT_EXEC_MARKERS.some((m) => lowered.includes(m))
}

function validateAnswers(a: OnboardingAnswers): string | null {
  if (!a.app_name.trim()) return 'App name is required.'
  if (a.app_name.length > 200) return 'App name must be at most 200 chars.'
  if (!APP_TYPES.includes(a.app_type)) return `App type must be one of: ${APP_TYPES.join('|')} (server: 422).`
  if (!CLOUD_PROVIDERS.includes(a.cloud_provider)) return `Cloud provider must be one of: ${CLOUD_PROVIDERS.join('|')} (server: 422).`
  if (!VOLUME_BANDS.includes(a.expected_eps)) return `Expected EPS must be one of: ${VOLUME_BANDS.join('|')} (server: 422).`
  if (!VOLUME_BANDS.includes(a.log_volume)) return `Log volume must be one of: ${VOLUME_BANDS.join('|')} (server: 422).`
  if (!Number.isInteger(a.retention_days) || a.retention_days < 1 || a.retention_days > 3650)
    return 'Retention must be an integer 1..3650 days (server: 422).'
  if (!a.alert_contact.trim()) return 'Alert contact is required.'
  if (a.alert_contact.length > 320) return 'Alert contact must be at most 320 chars.'
  if (a.service_endpoints.length > 32) return 'At most 32 service endpoints (server: 422).'
  for (const ep of a.service_endpoints) {
    if (!ep.trim()) return 'Service endpoints must not contain empty entries (server: 422).'
    if (ep.length > 500) return 'Each endpoint must be at most 500 chars (server: 422).'
    if (containsExecMarker(ep)) return `Endpoint “${ep}” contains disallowed content (server: 422).`
  }
  if (containsExecMarker(a.app_name)) return 'App name contains disallowed content (server: 422).'
  if (containsExecMarker(a.alert_contact)) return 'Alert contact contains disallowed content (server: 422).'
  return null
}

const EMPTY: OnboardingAnswers = {
  app_name: '',
  app_type: 'api',
  cloud_provider: 'aws',
  service_endpoints: [],
  expected_eps: '<100',
  log_volume: '<100',
  retention_days: 30,
  alert_contact: '',
}

export function WorkspaceOnboarding() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [step, setStep] = useState(0)
  const [answers, setAnswers] = useState<OnboardingAnswers>(EMPTY)
  const [endpointDraft, setEndpointDraft] = useState('')
  const [workspaceName, setWorkspaceName] = useState<string | null>(null)
  const [blocked, setBlocked] = useState(false)
  const [loading, setLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<OnboardingResult | null>(null)

  // Ownership gate: the id must belong to the caller. A 403 (unknown id OR
  // another user's id — the backend never distinguishes) renders a blocked
  // state with no workspace details, so URL tampering leaks nothing.
  useEffect(() => {
    if (!id) {
      setBlocked(true)
      setLoading(false)
      return
    }
    let cancelled = false
    const load = async () => {
      setLoading(true)
      setError(null)
      try {
        const ws = await getWorkspace(id)
        if (cancelled) return
        setWorkspaceName(ws.name)
        try {
          const saved = await getOnboarding(id)
          if (cancelled) return
          setAnswers(saved.answers)
          setResult(saved)
        } catch (e) {
          // 404 = wizard never submitted: start blank (not an error).
          const status = (e as { response?: { status?: number } })?.response?.status
          if (status !== 404) throw e
        }
      } catch (err) {
        if (cancelled) return
        const status = (err as { response?: { status?: number } })?.response?.status
        if (status === 403 || status === 404) setBlocked(true)
        else setError(apiError(err, 'Failed to load workspace.'))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [id])

  const set = <K extends keyof OnboardingAnswers>(key: K, value: OnboardingAnswers[K]) =>
    setAnswers((prev) => ({ ...prev, [key]: value }))

  const addEndpoint = () => {
    const ep = endpointDraft.trim()
    if (!ep) return
    set('service_endpoints', [...answers.service_endpoints, ep])
    setEndpointDraft('')
  }

  const handleSubmit = async () => {
    if (!id) return
    const clientError = validateAnswers(answers)
    if (clientError) {
      setError(clientError)
      return
    }
    setSubmitting(true)
    setError(null)
    try {
      const res = await submitOnboarding(id, answers)
      setResult(res)
      setStep(3)
    } catch (err) {
      setError(apiError(err, 'Submit failed.'))
    } finally {
      setSubmitting(false)
    }
  }

  if (loading) {
    return <div className="p-4 text-xs text-text-muted font-mono animate-pulse">Loading workspace…</div>
  }

  if (blocked || !id) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="card p-6 w-full max-w-md rounded-lg border border-[#2a2a2a] text-center">
          <div role="alert" className="text-sm text-text-primary font-mono">Workspace not found or access denied.</div>
          <div className="mt-1 text-xs text-text-muted font-mono">This workspace does not exist or belongs to another user.</div>
          <button onClick={() => navigate('/workspaces')} className={`${btnPrimary} mt-4`}>
            Back to workspaces
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="p-4 flex flex-col gap-4 max-w-2xl mx-auto w-full">
      <div>
        <h1 className="text-lg font-semibold text-text-primary">
          Onboarding{workspaceName ? ` — ${workspaceName}` : ''}
        </h1>
        <p className="text-xs text-text-muted font-mono">Guided setup — answers are validated against the same rules as the API.</p>
      </div>

      <ol className="flex gap-1" aria-label="Onboarding steps">
        {STEPS.map((label, i) => (
          <li
            key={label}
            aria-current={i === step ? 'step' : undefined}
            className={`flex-1 px-2 py-1.5 text-[11px] font-mono text-center rounded border ${
              i === step
                ? 'bg-accent-cyan/15 text-accent-cyan border-accent-cyan/30'
                : i < step
                  ? 'text-text-primary border-[#2a2a2a]'
                  : 'text-text-muted border-[#2a2a2a]'
            }`}
          >
            {i + 1}. {label}
          </li>
        ))}
      </ol>

      {error && (
        <div role="alert" className="px-3 py-2 text-xs font-mono rounded bg-red-900/20 border border-red-900/40 text-red-400">
          {error}
        </div>
      )}

      <div className="card rounded-lg border border-[#2a2a2a] p-4">
        {step === 0 && (
          <div className="space-y-4">
            <div>
              <label htmlFor="ob-app-name" className="block text-xs text-text-muted mb-1">App name</label>
              <input
                id="ob-app-name"
                value={answers.app_name}
                onChange={(e) => set('app_name', e.target.value)}
                placeholder="e.g. checkout-api"
                maxLength={200}
                className={inputClass}
              />
            </div>
            <div>
              <label htmlFor="ob-app-type" className="block text-xs text-text-muted mb-1">App type</label>
              <select
                id="ob-app-type"
                value={answers.app_type}
                onChange={(e) => set('app_type', e.target.value as AppType)}
                className={inputClass}
              >
                {APP_TYPES.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </div>
          </div>
        )}

        {step === 1 && (
          <div className="space-y-4">
            <div>
              <label htmlFor="ob-cloud" className="block text-xs text-text-muted mb-1">Cloud provider</label>
              <select
                id="ob-cloud"
                value={answers.cloud_provider}
                onChange={(e) => set('cloud_provider', e.target.value as CloudProvider)}
                className={inputClass}
              >
                {CLOUD_PROVIDERS.map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </div>
            <div>
              <label htmlFor="ob-endpoint" className="block text-xs text-text-muted mb-1">
                Service endpoints ({answers.service_endpoints.length}/32)
              </label>
              <div className="flex gap-2">
                <input
                  id="ob-endpoint"
                  value={endpointDraft}
                  onChange={(e) => setEndpointDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault()
                      addEndpoint()
                    }
                  }}
                  placeholder="https://checkout.internal:8080"
                  className={inputClass}
                />
                <button type="button" onClick={addEndpoint} className={btnPrimary}>Add</button>
              </div>
              <ul className="mt-2 space-y-1">
                {answers.service_endpoints.map((ep, i) => (
                  <li key={`${ep}-${i}`} className="flex items-center gap-2 text-xs font-mono text-text-primary">
                    <span className="truncate flex-1">{ep}</span>
                    <button
                      type="button"
                      onClick={() => set('service_endpoints', answers.service_endpoints.filter((_, j) => j !== i))}
                      className="text-red-400 hover:text-red-300"
                      aria-label={`Remove endpoint ${ep}`}
                    >
                      ✕
                    </button>
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <label htmlFor="ob-contact" className="block text-xs text-text-muted mb-1">Alert contact</label>
              <input
                id="ob-contact"
                value={answers.alert_contact}
                onChange={(e) => set('alert_contact', e.target.value)}
                placeholder="ops@example.com"
                maxLength={320}
                className={inputClass}
              />
            </div>
          </div>
        )}

        {step === 2 && (
          <div className="space-y-4">
            <div>
              <span id="ob-eps-label" className="block text-xs text-text-muted mb-1">Expected events/sec</span>
              <div role="radiogroup" aria-labelledby="ob-eps-label" className="flex gap-2">
                {VOLUME_BANDS.map((b) => (
                  <button
                    key={b}
                    type="button"
                    role="radio"
                    aria-checked={answers.expected_eps === b}
                    onClick={() => set('expected_eps', b)}
                    className={`flex-1 px-2 py-2 text-xs font-mono rounded border ${
                      answers.expected_eps === b
                        ? 'bg-accent-cyan/15 text-accent-cyan border-accent-cyan/40'
                        : 'text-text-muted border-[#2a2a2a] hover:text-text-primary'
                    }`}
                  >
                    {b}
                  </button>
                ))}
              </div>
            </div>
            <div>
              <span id="ob-log-label" className="block text-xs text-text-muted mb-1">Log volume band</span>
              <div role="radiogroup" aria-labelledby="ob-log-label" className="flex gap-2">
                {VOLUME_BANDS.map((b) => (
                  <button
                    key={b}
                    type="button"
                    role="radio"
                    aria-checked={answers.log_volume === b}
                    onClick={() => set('log_volume', b as VolumeBand)}
                    className={`flex-1 px-2 py-2 text-xs font-mono rounded border ${
                      answers.log_volume === b
                        ? 'bg-accent-cyan/15 text-accent-cyan border-accent-cyan/40'
                        : 'text-text-muted border-[#2a2a2a] hover:text-text-primary'
                    }`}
                  >
                    {b}
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label htmlFor="ob-retention" className="block text-xs text-text-muted mb-1">Retention (days, 1–3650)</label>
              <input
                id="ob-retention"
                type="number"
                min={1}
                max={3650}
                value={answers.retention_days}
                onChange={(e) => set('retention_days', Number(e.target.value))}
                className={inputClass}
              />
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-3">
            <dl className="text-xs font-mono space-y-1">
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">app_name</dt><dd className="text-text-primary truncate">{answers.app_name || '—'}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">app_type</dt><dd className="text-text-primary">{answers.app_type}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">cloud_provider</dt><dd className="text-text-primary">{answers.cloud_provider}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">endpoints</dt><dd className="text-text-primary">{answers.service_endpoints.length} configured</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">expected_eps</dt><dd className="text-text-primary">{answers.expected_eps}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">log_volume</dt><dd className="text-text-primary">{answers.log_volume}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">retention_days</dt><dd className="text-text-primary">{answers.retention_days}</dd></div>
              <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">alert_contact</dt><dd className="text-text-primary truncate">{answers.alert_contact || '—'}</dd></div>
            </dl>
            {result && (
              <div className="rounded border border-accent-cyan/30 bg-accent-cyan/5 p-3">
                <div className="text-[11px] uppercase tracking-widest text-accent-cyan font-mono mb-2">
                  Suggested config (advisory only — nothing applied)
                </div>
                <dl className="text-xs font-mono space-y-1">
                  <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">queue_depth</dt><dd className="text-text-primary">{result.suggested_config.queue_depth}</dd></div>
                  <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">batch_size</dt><dd className="text-text-primary">{result.suggested_config.batch_size}</dd></div>
                  <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">poll_interval_s</dt><dd className="text-text-primary">{result.suggested_config.poll_interval_s}</dd></div>
                  <div className="flex gap-2"><dt className="text-text-muted w-36 shrink-0">scrape_interval_s</dt><dd className="text-text-primary">{result.suggested_config.scrape_interval_s}</dd></div>
                </dl>
                <div className="mt-2 text-[11px] text-text-muted font-mono truncate">config: {result.config_path}</div>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="flex items-center gap-2">
        {step > 0 && (
          <button onClick={() => setStep((s) => s - 1)} className={btnGhost}>Back</button>
        )}
        <div className="flex-1" />
        {step < 3 && (
          <button onClick={() => setStep((s) => s + 1)} className={btnPrimary}>Next</button>
        )}
        {step === 3 && (
          <>
            <button onClick={() => navigate('/workspaces')} className={btnGhost}>
              {result ? 'Done' : 'Cancel'}
            </button>
            <button onClick={() => void handleSubmit()} disabled={submitting} className={btnPrimary}>
              {submitting ? 'Saving…' : result ? 'Resubmit' : 'Finish setup'}
            </button>
            {result && (
              <button onClick={() => navigate('/')} className={btnPrimary}>
                Enter dashboard
              </button>
            )}
          </>
        )}
      </div>
    </div>
  )
}
