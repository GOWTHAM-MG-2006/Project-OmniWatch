/**
 * OmniWatch — Dashboard Frontend
 * Component: CopilotDrawer
 * Phase: 11
 * Purpose: Slide-out drawer for streaming copilot chat with LLM
 * Inputs: POST /api/copilot (streaming SSE via useStream), open/close state
 * Outputs: Drawer UI with conversation history + live StreamingMessage response
 */

import { useState, useEffect, useRef, useMemo } from 'react'
import api from '../api/client'
import { useStream } from '../hooks/useStream'
import { StreamingMessage } from './StreamingMessage'
import { RightDrawer } from './RightDrawer'

interface CopilotDrawerProps {
  open: boolean
  onClose: () => void
}

interface Message {
  role: 'user' | 'assistant'
  text: string
}

/**
 * Trigger start() when pendingQuestion changes.
 * useStream captures body in useCallback deps, so a new body
 * produces a new `start`. This effect bridges the gap.
 */
function useTriggerStart(
  pendingQuestion: string | null,
  start: () => void,
) {
  useEffect(() => {
    if (pendingQuestion !== null) {
      start()
    }
  }, [pendingQuestion, start])
}

/**
 * When streaming finishes (isStreaming goes false with streamText),
 * commit the accumulated response into the messages array.
 */
function useCommitOnFinish(
  isStreaming: boolean,
  streamText: string,
  pendingQuestion: string | null,
  setMessages: React.Dispatch<React.SetStateAction<Message[]>>,
  setPendingQuestion: React.Dispatch<React.SetStateAction<string | null>>,
) {
  const wasStreamingRef = useRef(false)
  useEffect(() => {
    if (wasStreamingRef.current && !isStreaming && streamText) {
      setMessages((prev) => [...prev, { role: 'assistant', text: streamText }])
      setPendingQuestion(null)
    }
    wasStreamingRef.current = isStreaming
  }, [isStreaming, streamText, pendingQuestion, setMessages, setPendingQuestion])
}

export function CopilotDrawer({ open, onClose }: CopilotDrawerProps) {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [pendingQuestion, setPendingQuestion] = useState<string | null>(null)
  const [activeModel, setActiveModel] = useState<string>('')
  const [activeProvider, setActiveProvider] = useState<string>('')

  // Fetch active model for the header bar (updates on open and after each answer)
  useEffect(() => {
    if (!open) return
    api
      .get<{ provider: string; model_name: string }>('/config/model-settings')
      .then(({ data }) => {
        setActiveModel(data.model_name || '')
        setActiveProvider(data.provider || '')
      })
      .catch(() => {})
  }, [open, messages.length])

  // Derive body for useStream — undefined until user sends
  // useMemo prevents new object identity on every render, which would
  // cause useStream's useCallback to produce a new `start` ref,
  // retriggering useTriggerStart's effect in an infinite loop.
  const body = useMemo(
    () =>
      pendingQuestion !== null
        ? {
            question: pendingQuestion,
            stream: true as const,
            history: messages.slice(-6).map((m) => ({ role: m.role, content: m.text.slice(0, 2000) })),
          }
        : undefined,
    [pendingQuestion, messages],
  )

  const { streamText, isStreaming, error, start } = useStream(
    '/api/copilot',
    body,
  )

  // Bridge: trigger start() when pendingQuestion changes
  useTriggerStart(pendingQuestion, start)

  // Commit streamed response to messages when streaming finishes
  useCommitOnFinish(isStreaming, streamText, pendingQuestion, setMessages, setPendingQuestion)

  const handleSend = () => {
    if (!input.trim() || isStreaming) return
    setMessages((prev) => [...prev, { role: 'user', text: input }])
    setInput('')
    setPendingQuestion(input.trim())
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <RightDrawer open={open} onClose={onClose} title="Ops Copilot">
      <div className="flex flex-col" style={{ height: 'calc(100vh - 8rem)' }}>
        {/* Memory bar */}
        {messages.length > 0 && (
          <div className="flex items-center justify-between px-3 py-1.5 rounded-lg bg-bg-deep/60 border border-border-default mb-2 text-xs text-text-muted">
            <span>
              Context: {messages.length} message{messages.length !== 1 ? 's' : ''} • ~{messages.reduce((a, m) => a + Math.ceil(m.text.length / 4), 0)} tokens
            </span>
            <button
              onClick={() => setMessages([])}
              className="text-text-muted hover:text-error transition-colors"
            >
              Clear
            </button>
          </div>
        )}
        {/* Active Model bar */}
        {activeModel && (
          <div className="flex items-center justify-between px-3 py-1.5 rounded-lg bg-accent-cyan/10 border border-accent-cyan/20 mb-3 text-xs font-mono">
            <span className="text-accent-cyan">Active Model: {activeModel}</span>
            <span className="text-text-muted">{activeProvider}</span>
          </div>
        )}

        {/* Message history */}
        <div className="flex-1 overflow-y-auto space-y-3 mb-4">
          {messages.length === 0 && !isStreaming && (
            <div className="text-text-muted text-sm text-center mt-8">
              Ask anything about your infrastructure, incidents, or anomalies.
            </div>
          )}

          {messages.map((msg, i) => (
            <div
              key={`${msg.role}-${i}`}
              className={`p-3 rounded-xl text-sm ${
                msg.role === 'user'
                  ? 'bg-accent-cyan/10 text-text-primary ml-8'
                  : 'bg-bg-deep text-text-primary mr-8'
              }`}
            >
              {msg.role === 'assistant' ? (
                <StreamingMessage text={msg.text} isStreaming={false} />
              ) : (
                <span className="whitespace-pre-wrap">{msg.text}</span>
              )}
            </div>
          ))}

          {/* Live streaming response */}
          {pendingQuestion !== null && (
            <div className="p-3 rounded-xl bg-bg-deep text-text-primary mr-8">
              <StreamingMessage text={streamText} isStreaming={isStreaming} />
            </div>
          )}

          {error && (
            <div className="p-3 rounded-xl bg-error/10 text-error text-sm">
              {error}
            </div>
          )}
        </div>

        {/* Input */}
        <div className="border-t border-border-default pt-3">
          <div className="flex gap-2">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask the copilot..."
              disabled={isStreaming}
              className="flex-1 bg-bg-deep border border-border-default rounded-xl px-3 py-2 text-sm text-text-primary placeholder:text-text-muted focus:outline-none focus:border-accent-cyan disabled:opacity-50"
            />
            <button
              onClick={handleSend}
              disabled={isStreaming || !input.trim()}
              className="px-4 py-2 bg-accent-cyan text-bg-deep rounded-xl text-sm font-medium hover:opacity-90 disabled:opacity-50 transition-opacity"
            >
              {isStreaming ? '...' : 'Send'}
            </button>
          </div>
        </div>
      </div>
    </RightDrawer>
  )
}
