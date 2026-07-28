# OmniWatch — AI-Driven Cloud Operations (AIOps) Platform

Proactive anomaly detection, causal root cause analysis, automated remediation, and self-healing for cloud-native environments.

**Competition:** IEEE YESIST12 2026 — IEngage Track  
**Stack:** Python 3.11+ | Go | FastAPI | Kafka | Flink | ClickHouse | Neo4j | MinIO | Merlion | PyRCA | OPA | Ollama | React  

---

## Architecture (12-Phase Build)

| Phase | Layer | Description |
|-------|-------|-------------|
| 0 | Environment Setup | GCP + GKE + Docker + Terraform |
| 1 | Microservices + OTel | Real services with OpenTelemetry SDKs |
| 2 | Telemetry Ingestion | OTel Collector → Kafka → Flink streaming |
| 3 | Entity Resolution | Cross-cloud ID normalization |
| 4 | Windowing + Feature Store | Windowed aggregation, Redis feature store |
| 5 | Unified Storage | ClickHouse (metrics), Neo4j (graph), MinIO (objects) |
| 6 | Predictive Intelligence | Merlion anomaly detection + Security classifier |
| 7 | Causal Graph Engine | PyRCA + DAG traversal root cause analysis |
| 8 | Incident Prioritization | P1-P4 severity + alert deduplication |
| 9 | Orchestration + Policy | OPA + action library + auto-remediation |
| 10 | Generative AI | Grounded LLM + compliance reports |
| 11 | Dashboard + Learning | React dashboard + continuous learning loop |

**Full pipeline:** Services → OTel → Kafka → Flink → Entity Resolution → Windowing → ClickHouse/Neo4j → ML → Causal RCA → OPA → Auto-Remediation → Dashboard

---

## Quick Start

```bash
# 1. Prerequisites
#    - Docker Desktop
#    - Python 3.11+
#    - GCP free tier account (for K8s deployment)

# 2. Start local infrastructure
docker-compose up -d

# 3. Start external simulation (separate repo)
cd E:\Telementry-Simulation\opentelemetry-demo
./deploy.sh docker

# 4. Verify services
#    Neo4j:   http://localhost:7474  (neo4j/omniwatch)
#    Kafka:   localhost:9092
#    Jaeger:  http://localhost:16686
#    Prometheus: http://localhost:9090
```

---

## Development

Build phases are strictly gated — Phase N must pass before Phase N+1 begins.
See `.omo/plans/OmniWatch-Built Plan.md` for the complete build guide.

### Per-Phase Deliverables
1. Working code in the phase directory
2. E2E test in `tests/phase-N-e2e/`
3. Phase summary saved to `Project_Source_Files/Phase_Summary_Details/System-Architecture/Phase-N-summary.md`

### Key Documents
- **Build Plan:** `.omo/plans/OmniWatch-Built Plan.md`
- **Architecture:** `Project_Source_Files/System-Architecture/OmniWatch-DataFlow-ASCII.md`
- **Data Flow:** `Project_Source_Files/My-R&D-Files/Dataflow.md` (22-tool pipeline)
- **Gaps to Address:** `Project_Source_Files/My-R&D-Files/PRIMARY GAPS TO ADDRESS - OMNIWATCH.txt`

---

## External Dependencies

| Dependency | Location | Purpose |
|-----------|----------|---------|
| OpenTelemetry Demo | `E:\Telementry-Simulation\opentelemetry-demo/` | 12 microservices providing OTLP telemetry |
| GCP Free Tier | Cloud Console | GKE cluster, Artifact Registry, IAM |

---

## License

Internal project — IEEE YESIST12 2026 competition entry.