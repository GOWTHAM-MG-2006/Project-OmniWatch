/**
 * OmniWatch — Dashboard Frontend
 * Component: ModelTestResult
 * Phase: 3.3 (model-manager)
 * Purpose: Reusable card showing LLM model test-connection results — success/failure with latency
 * Inputs: Props — success, provider, model, latencyMs, error?
 * Outputs: Styled status card with icon, provider/model labels, latency badge, error message
 */

export interface ModelTestResultProps {
  success: boolean
  provider: string
  model: string
  latencyMs: number
  error?: string
}

export function ModelTestResult({
  success,
  provider,
  model,
  latencyMs,
  error,
}: ModelTestResultProps) {
  return (
    <div
      className={
        'flex flex-col gap-3 p-4 rounded-xl border transition-all duration-200 ' +
        (success
          ? 'border-[rgba(34,197,94,0.3)] shadow-[0_0_15px_rgba(34,197,94,0.08)]'
          : 'border-[rgba(239,68,68,0.3)] shadow-[0_0_15px_rgba(239,68,68,0.08)]')
      }
      style={{
        background: success
          ? 'linear-gradient(135deg, #1a1a1a, #141816)'
          : 'linear-gradient(135deg, #1a1a1a, #1a1416)',
      }}
    >
      {/* Header row: icon + status */}
      <div className="flex items-center gap-3">
        <div
          className={
            'flex items-center justify-center w-8 h-8 rounded-full text-sm ' +
            (success
              ? 'bg-[rgba(34,197,94,0.15)] text-[#22c55e]'
              : 'bg-[rgba(239,68,68,0.15)] text-[#ef4444]')
          }
        >
          {success ? '✓' : '✕'}
        </div>
        <div className="flex flex-col">
          <span className="text-text-primary text-sm font-medium">
            {success ? 'Connection Successful' : 'Connection Failed'}
          </span>
          <span className="text-text-muted text-xs font-mono">
            {provider} · {model}
          </span>
        </div>
      </div>

      {/* Latency badge */}
      <div className="flex items-center gap-2">
        <span
          className={
            'inline-flex items-center px-2 py-0.5 rounded-md text-xs font-mono ' +
            (success
              ? 'bg-[rgba(34,197,94,0.1)] text-[#22c55e]'
              : 'bg-[rgba(239,68,68,0.1)] text-[#ef4444]')
          }
        >
          {latencyMs}ms
        </span>
        {success && (
          <span className="text-text-muted text-[10px] font-mono uppercase tracking-wider">
            latency
          </span>
        )}
      </div>

      {/* Error message (conditional) */}
      {error && (
        <div className="flex items-start gap-2 px-3 py-2 rounded-lg bg-[rgba(239,68,68,0.08)] border border-[rgba(239,68,68,0.2)]">
          <span className="text-[#ef4444] text-xs shrink-0">⚠</span>
          <span className="text-[#ef4444] text-xs font-mono leading-relaxed">
            {error}
          </span>
        </div>
      )}
    </div>
  )
}

export default ModelTestResult
