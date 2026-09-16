/**
 * OmniWatch — Dashboard Frontend
 * Component: useStream hook
 * Phase: 11
 * Purpose: SSE consumption hook for streaming copilot responses
 * Inputs: URL + optional POST body
 * Outputs: { streamText, isStreaming, error, start }
 */

import { useState, useCallback, useRef, useEffect } from 'react'

/** SSE token chunk from backend — matches `data: {"token":"...","done":false}` */
interface StreamChunk {
  token: string
  done: boolean
}

interface UseStreamResult {
  /** Accumulated streamed text */
  streamText: string
  /** Whether a stream is actively being consumed */
  isStreaming: boolean
  /** Error message if the stream failed, null otherwise */
  error: string | null
  /** Start a new stream. Resets previous state. */
  start: () => void
}

/**
 * Parse a single SSE `data:` line into a StreamChunk.
 * Handles our backend format `{"token":"...","done":false}`
 * and falls back to OpenAI-style `choices[0].delta.content`.
 */
function parseSSELine(line: string): StreamChunk | null {
  const trimmed = line.trim()
  if (!trimmed.startsWith('data:')) return null
  const payload = trimmed.slice(5).trim()
  if (!payload || payload === '[DONE]') return { token: '', done: true }

  try {
    const json = JSON.parse(payload) as Record<string, unknown>
    // OmniWatch native format: { token, done }
    if ('done' in json && 'token' in json) {
      return { token: String(json.token), done: Boolean(json.done) }
    }
    // OpenAI-style fallback: choices[0].delta.content
    if ('choices' in json) {
      const choices = json.choices as Array<Record<string, unknown>>
      const delta = (choices[0]?.delta ?? null) as Record<string, unknown> | null
      const content = delta?.content
      if (typeof content === 'string') {
        return { token: content, done: false }
      }
    }
  } catch {
    // Non-JSON data line — treat as raw text token
    return { token: payload, done: false }
  }
  return null
}

/**
 * Consume an SSE response body as an async generator of StreamChunk.
 * Reads bytes from ReadableStream, decodes to text, splits on newlines,
 * and yields parsed SSE data lines.
 */
async function* consumeSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<StreamChunk> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      // Keep the last potentially-incomplete line in the buffer
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        const chunk = parseSSELine(line)
        if (chunk) yield chunk
      }
    }
    // Process remaining buffer
    if (buffer.trim()) {
      const chunk = parseSSELine(buffer)
      if (chunk) yield chunk
    }
  } finally {
    reader.releaseLock()
  }
}

/**
 * Custom hook for consuming SSE streams.
 *
 * Uses `fetch` + `ReadableStream` for POST requests (the primary use case
 * for copilot streaming). For GET-only scenarios, `EventSource` is the
 * native alternative but does not support request bodies — callers that
 * only need GET streaming can swap in `new EventSource(url)` directly.
 *
 * @param url  Endpoint path — relative (e.g. `/api/copilot`) works with
 *             the Vite dev proxy, same as the axios client in `client.ts`.
 * @param body Optional JSON body for POST streaming.
 */
export function useStream(url: string, body?: object): UseStreamResult {
  const [streamText, setStreamText] = useState('')
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  // Cleanup on unmount — abort any in-flight request
  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  const start = useCallback(() => {
    // Abort any previous stream
    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller

    // Reset state
    setStreamText('')
    setError(null)
    setIsStreaming(true)

    const run = async () => {
      try {
        const fetchOptions: RequestInit = {
          method: body ? 'POST' : 'GET',
          signal: controller.signal,
          headers: body ? { 'Content-Type': 'application/json' } : undefined,
          body: body ? JSON.stringify(body) : undefined,
        }

        const response = await fetch(url, fetchOptions)

        if (!response.ok) {
          throw new Error(
            `Stream request failed: ${response.status} ${response.statusText}`,
          )
        }

        const contentType = response.headers.get('content-type') ?? ''

        // SSE via POST returns text/event-stream with a ReadableStream body
        if (contentType.includes('text/event-stream') && response.body) {
          let acc = ''
          for await (const chunk of consumeSSE(response.body)) {
            if (controller.signal.aborted) break
            if (chunk.done) break
            acc += chunk.token
            setStreamText(acc)
          }
        } else {
          // Non-streaming fallback — read full JSON response
          const json = (await response.json()) as Record<string, unknown>
          const text = String(
            json.answer ?? json.response ?? json.text ?? json.content ?? JSON.stringify(json),
          )
          setStreamText(text)
        }
      } catch (err) {
        // Ignore abort — caller intentionally cancelled
        if (err instanceof DOMException && err.name === 'AbortError') return
        if (controller.signal.aborted) return
        setError(err instanceof Error ? err.message : 'Stream failed')
      } finally {
        if (!controller.signal.aborted) {
          setIsStreaming(false)
        }
      }
    }

    void run()
  }, [url, body])

  return { streamText, isStreaming, error, start }
}
