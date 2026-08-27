/**
 * OmniWatch — Dashboard Frontend
 * Component: TopologyMini
 * Phase: 11
 * Purpose: Read-only React Flow minimap showing service topology
 */

import { ReactFlow, Background, MiniMap, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'

const STATUS_STYLES: Record<string, string> = {
  healthy: '#22c55e',
  warning: '#f59e0b',
  critical: '#ef4444',
  unknown: '#6b7280',
}

interface TopologyEdgeRaw {
  source: string
  target: string
  label: string
  data: { latency_p50: number; error_rate: number }
}

interface TopologyMiniProps {
  nodes: Node[]
  edges: TopologyEdgeRaw[]
}

function ServiceNode({ data }: { data: Record<string, unknown> }) {
  const status = (data.status as string) ?? 'unknown'
  const color = STATUS_STYLES[status] ?? STATUS_STYLES.unknown
  return (
    <div
      className="px-3 py-2 rounded-lg border text-xs text-center min-w-[100px]"
      style={{ borderColor: color, background: '#1a1a1a' }}
    >
      <div className="text-text-primary font-medium truncate">{data.label as string}</div>
      <div className="text-text-muted text-[10px] mt-0.5">{data.entity_type as string}</div>
      <div className="mt-1 h-1 rounded-full" style={{ background: color, width: '100%' }} />
    </div>
  )
}

const nodeTypes = { serviceNode: ServiceNode }

export function TopologyMini({ nodes, edges }: TopologyMiniProps) {
  if (!nodes.length) {
    return (
      <div className="h-full flex items-center justify-center text-text-muted text-sm">
        No topology data
      </div>
    )
  }

  const rfEdges = edges.map((e, i) => ({
    id: `e-${i}`,
    source: e.source,
    target: e.target,
    label: e.label,
    data: e.data,
  }))

  return (
    <ReactFlow
      nodes={nodes}
      edges={rfEdges}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.3 }}
      proOptions={{ hideAttribution: true }}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      className="bg-bg-deep rounded-lg"
    >
      <Background color="#2a2a2a" gap={24} />
      <MiniMap
        nodeColor={(n) => {
          const s = (n.data?.status as string) ?? 'unknown'
          return STATUS_STYLES[s] ?? STATUS_STYLES.unknown
        }}
        maskColor="rgba(15,15,15,0.7)"
        style={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8 }}
      />
    </ReactFlow>
  )
}
