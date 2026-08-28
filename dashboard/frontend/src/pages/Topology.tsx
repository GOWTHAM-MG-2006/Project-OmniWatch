/**
 * OmniWatch — Dashboard Frontend
 * Component: Topology Page
 * Phase: 11
 * Purpose: Full-screen interactive React Flow topology graph from Neo4j — Stitch design polished
 * Inputs: Dashboard API — /api/topology
 * Outputs: Interactive graph with node status coloring and edge labels
 */

import { ReactFlow, Background, Controls, MiniMap, type Node, type Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useFetch } from '../hooks/useFetch'
import { fetchTopology } from '../api/client'

const STATUS_STYLES: Record<string, { border: string; bg: string; shadow: string }> = {
  healthy: { border: '#22c55e', bg: 'linear-gradient(135deg, #0a1f0a, #0f2d0f)', shadow: '0 0 12px rgba(34,197,94,0.2)' },
  warning: { border: '#f59e0b', bg: 'linear-gradient(135deg, #1f1a0a, #2d2710)', shadow: '0 0 12px rgba(245,158,11,0.2)' },
  critical: { border: '#ef4444', bg: 'linear-gradient(135deg, #1f0a0a, #2d1010)', shadow: '0 0 12px rgba(239,68,68,0.2)' },
  unknown: { border: '#6b7280', bg: 'linear-gradient(135deg, #1a1a1a, #141618)', shadow: 'none' },
}

interface ServiceNodeData {
  label: string
  entity_type: string
  criticality: string
  status: string
  anomaly_score: number
  [key: string]: unknown
}

function ServiceNode({ data }: { data: ServiceNodeData }) {
  const status = data.status || 'unknown'
  const style = STATUS_STYLES[status] ?? STATUS_STYLES.unknown
  return (
    <div
      className="px-4 py-3 rounded-xl border-2 text-xs text-center min-w-[130px] transition-all duration-200 hover:scale-[1.03]"
      style={{
        borderColor: style.border,
        background: style.bg,
        boxShadow: style.shadow,
      }}
    >
      <div className="text-[#e4e4e7] font-heading text-sm" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
        {data.label}
      </div>
      <div className="text-[#a1a1aa] text-[10px] mt-0.5">{data.entity_type}</div>
      {data.criticality && (
        <div className="text-[#a1a1aa] text-[10px] mt-0.5">Criticality: {data.criticality}</div>
      )}
      <div className="mt-2 h-1.5 rounded-full overflow-hidden" style={{ background: '#2a2a2a' }}>
        <div
          className="h-full rounded-full"
          style={{
            width: `${Math.min((data.anomaly_score ?? 0) * 100, 100)}%`,
            background: `linear-gradient(90deg, ${style.border}88, ${style.border})`,
            boxShadow: `0 0 6px ${style.border}44`,
          }}
        />
      </div>
      <div className="text-[10px] mt-1.5 font-mono" style={{ color: style.border }}>
        Score: {data.anomaly_score?.toFixed(2) ?? '0.00'}
      </div>
    </div>
  )
}

const nodeTypes = { serviceNode: ServiceNode }

export function Topology() {
  const { data, loading, error } = useFetch(fetchTopology)

  const nodes: Node[] = data?.nodes ?? []
  const edges: Edge[] = (data?.edges ?? []).map((e, i) => ({
    id: `e-${i}`,
    source: e.source,
    target: e.target,
    label: e.label,
    data: e.data,
  }))

  return (
    <div className="p-4 h-full flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-[#e4e4e7]" style={{ fontFamily: "'Space Grotesk', sans-serif" }}>
            Service Topology
          </h1>
          <p className="text-[#a1a1aa] text-xs font-mono">
            {data?.node_count ?? 0} nodes, {data?.edge_count ?? 0} edges
          </p>
        </div>
        <div className="flex gap-3 text-[10px] text-[#a1a1aa]">
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ background: '#22c55e', boxShadow: '0 0 6px rgba(34,197,94,0.4)' }} />
            Healthy
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ background: '#f59e0b', boxShadow: '0 0 6px rgba(245,158,11,0.4)' }} />
            Warning
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ background: '#ef4444', boxShadow: '0 0 6px rgba(239,68,68,0.4)' }} />
            Critical
          </span>
        </div>
      </div>

      <div className="card flex-1 overflow-hidden rounded-lg border border-[#2a2a2a]">
        {loading ? (
          <div className="h-full flex items-center justify-center text-[#a1a1aa] text-sm font-mono animate-pulse">
            Loading topology...
          </div>
        ) : error ? (
          <div className="h-full flex items-center justify-center text-[#ef4444] text-sm font-mono">{error}</div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            proOptions={{ hideAttribution: true }}
            className="bg-[#0f0f11]"
          >
            <Background color="#2a2a2a" gap={24} />
            <Controls
              style={{ background: '#1a1a1a', borderColor: '#2a2a2a', borderRadius: 8 }}
            />
            <MiniMap
              nodeColor={(n) => {
                const s = (n.data?.status as string) ?? 'unknown'
                return STATUS_STYLES[s]?.border ?? STATUS_STYLES.unknown.border
              }}
              maskColor="rgba(15,15,17,0.7)"
              style={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8 }}
            />
          </ReactFlow>
        )}
      </div>
    </div>
  )
}
