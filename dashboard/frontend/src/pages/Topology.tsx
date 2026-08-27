/**
 * OmniWatch — Dashboard Frontend
 * Component: Topology Page
 * Phase: 11
 * Purpose: Full-screen interactive React Flow topology graph from Neo4j
 * Inputs: Dashboard API — /api/topology
 * Outputs: Interactive graph with node status coloring and edge labels
 */

import { ReactFlow, Background, Controls, MiniMap, type Node, type Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useFetch } from '../hooks/useFetch'
import { fetchTopology } from '../api/client'

const STATUS_STYLES: Record<string, { border: string; bg: string }> = {
  healthy: { border: '#22c55e', bg: '#0a1f0a' },
  warning: { border: '#f59e0b', bg: '#1f1a0a' },
  critical: { border: '#ef4444', bg: '#1f0a0a' },
  unknown: { border: '#6b7280', bg: '#1a1a1a' },
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
      className="px-4 py-3 rounded-lg border-2 text-xs text-center min-w-[120px] shadow-lg"
      style={{ borderColor: style.border, background: style.bg }}
    >
      <div className="text-text-primary font-heading text-sm">{data.label}</div>
      <div className="text-text-muted text-[10px] mt-0.5">{data.entity_type}</div>
      {data.criticality && (
        <div className="text-text-muted text-[10px]">Criticality: {data.criticality}</div>
      )}
      <div className="mt-1.5 h-1.5 rounded-full" style={{ background: style.border, width: '100%' }} />
      <div className="text-[10px] mt-1 font-mono" style={{ color: style.border }}>
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
    <div className="p-4 h-full flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-heading text-lg text-text-primary">Service Topology</h1>
          <p className="text-text-muted text-xs">
            {data?.node_count ?? 0} nodes, {data?.edge_count ?? 0} edges
          </p>
        </div>
        <div className="flex gap-3 text-[10px] text-text-muted">
          <span><span className="inline-block w-2 h-2 rounded-full bg-status-healthy mr-1" />Healthy</span>
          <span><span className="inline-block w-2 h-2 rounded-full bg-status-warning mr-1" />Warning</span>
          <span><span className="inline-block w-2 h-2 rounded-full bg-status-critical mr-1" />Critical</span>
        </div>
      </div>

      <div className="card flex-1 overflow-hidden">
        {loading ? (
          <div className="h-full flex items-center justify-center text-text-muted text-sm">Loading topology...</div>
        ) : error ? (
          <div className="h-full flex items-center justify-center text-status-critical text-sm">{error}</div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            proOptions={{ hideAttribution: true }}
            className="bg-bg-deep"
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
              maskColor="rgba(15,15,15,0.7)"
              style={{ background: '#1a1a1a', border: '1px solid #2a2a2a', borderRadius: 8 }}
            />
          </ReactFlow>
        )}
      </div>
    </div>
  )
}
