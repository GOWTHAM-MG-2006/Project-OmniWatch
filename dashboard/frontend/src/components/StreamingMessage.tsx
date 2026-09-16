/**
 * OmniWatch — Dashboard Frontend
 * Component: StreamingMessage
 * Phase: 11
 * Purpose: Renders streaming LLM text with typewriter feel and auto-scroll
 * Inputs: text (accumulated output), isStreaming (true while tokens arrive)
 * Outputs: <div> with pre-wrapped text + blinking cursor
 */

import { useEffect, useRef } from 'react'

interface StreamingMessageProps {
  text: string
  isStreaming: boolean
}

export function StreamingMessage({ text, isStreaming }: StreamingMessageProps) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [text])

  return (
    <div className="text-sm font-mono text-text-primary whitespace-pre-wrap leading-relaxed">
      {text}
      {isStreaming && (
        <span
          className="inline-block w-[2px] h-[1em] bg-accent-cyan align-middle ml-0.5 animate-pulse"
          aria-hidden="true"
        />
      )}
      <div ref={bottomRef} />
    </div>
  )
}
