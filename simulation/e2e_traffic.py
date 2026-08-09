"""
OmniWatch — Simulation
Component: e2e_traffic
Phase: 2
Purpose: End-to-end verification harness. Drives continuous business traffic to
         the order/user services while a database_cascade anomaly scenario is
         injected, then asserts the full telemetry pipeline (otelcol -> kafka ->
         Flink entity-resolution -> ClickHouse -> Merlion -> causal -> prioritization)
         emits metrics, anomalies, and an incident.
Inputs:  running docker-compose stack (otelcol, kafka, clickhouse, neo4j, services).
Outputs: JSON KPI summary on stdout; exits 0 if incidents>0 else 1.
"""
import json, os, subprocess, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def sh(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()
    except Exception as e:
        return f"ERR {e}"


def ch_count(table):
    out = sh(["docker", "exec", "omniwatch-clickhouse", "clickhouse-client", "--query",
              f"SELECT count() FROM omniwatch.{table} FORMAT TabSeparated"], timeout=15)
    lines = [l for l in out.splitlines() if l.strip().strip(".") != ""]
    try:
        return int(lines[-1])
    except Exception:
        return out


def kafka_end_offset(topic):
    out = sh(["docker", "exec", "omniwatch-kafka", "kafka-get-offsets",
              "--bootstrap-server", "kafka:29092", "--topic", topic, "--time", "-1"], timeout=15)
    best = 0
    for line in out.splitlines():
        p = line.split(":")
        if len(p) >= 2:
            try:
                best = max(best, int(p[-1]))
            except ValueError:
                pass
    return best


def otelcol_services_since(sec=30):
    out = sh(["docker", "logs", "omniwatch-otelcol", f"--since={sec}s"], timeout=15)
    return [s for s in ("order-service", "user-service", "api-gateway") if s in out]


def neo4j_counts():
    def q(cypher):
        return sh(["docker", "exec", "omniwatch-neo4j", "cypher-shell", "-u", "neo4j",
                   "-p", "omniwatch", "--format", "plain",
                   "--", cypher], timeout=15)
    n = q("MATCH (n) RETURN count(n)")
    r = q("MATCH ()-[r]->() RETURN count(r)")
    return {"nodes": n, "rels": r}


def inject_scenario(scenario, ttl=90):
    """POST {scenario,ttl_seconds} to /__inject/anomaly on api-gateway/user/order."""
    for port in ("8000", "8001", "8002"):
        try:
            data = json.dumps({"scenario": scenario, "ttl_seconds": ttl}).encode()
            req = urllib.request.Request(f"http://localhost:{port}/__inject/anomaly",
                                         data=data,
                                         headers={"Content-Type": "application/json"},
                                         method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
    return f"injected {scenario} ttl={ttl}s to :8000,:8001,:8002"


def main():
    tg_log = os.path.join(ROOT, "simulation", "traffic_run.log")
    tf = open(tg_log, "w")
    proc = subprocess.Popen([PY, "-u",
                             os.path.join(ROOT, "simulation", "traffic_generator.py"),
                             "--rps", "30", "--duration", "210", "--seed-users", "3"],
                            stdout=tf, stderr=subprocess.STDOUT)
    progress = []
    try:
        time.sleep(12)  # baseline traffic establishes normal metric baseline
        progress.append({"phase": "baseline", "t": 12,
                         "ch_metrics": ch_count("metrics"),
                         "ch_anomalies": ch_count("anomalies"),
                         "ch_incidents": ch_count("incidents"),
                         "otelcol_services": otelcol_services_since(30),
                         "kafka_metrics_offset": kafka_end_offset("omniwatch.metrics.raw")})
        progress.append({"phase": "injected", "t": 12,
                         "injection": inject_scenario("database_cascade", 90)})
        for i in range(10):
            time.sleep(15)
            row = {"phase": f"poll{i+1}", "t": 12 + (i + 1) * 15,
                   "ch_metrics": ch_count("metrics"),
                   "ch_anomalies": ch_count("anomalies"),
                   "ch_incidents": ch_count("incidents"),
                   "kafka_metrics": kafka_end_offset("omniwatch.metrics.raw"),
                   "kafka_anomalies": kafka_end_offset("omniwatch.anomalies.detected"),
                   "kafka_incidents": kafka_end_offset("omniwatch.incidents.causal")}
            progress.append(row)
            sys.stderr.write("[poll %d] t=%ds metrics=%s anomalies=%s incidents=%s "
                             "kafka_m=%s kafka_a=%s kafka_i=%s\n" % (
                                 i + 1, row["t"], row["ch_metrics"], row["ch_anomalies"],
                                 row["ch_incidents"], row["kafka_metrics"],
                                 row["kafka_anomalies"], row["kafka_incidents"]))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        tf.close()

    with open(tg_log) as fh:
        tail = "\n".join(fh.read().splitlines()[-12:])

    final = progress[-1] if progress else {}
    summary = {
        "baseline": progress[0] if progress else None,
        "injection": progress[1]["injection"] if len(progress) > 1 else None,
        "polls": progress[2:],
        "final": final,
        "neo4j": neo4j_counts(),
        "traffic_log_tail": tail,
    }
    print(json.dumps(summary, indent=2, default=str))
    ok = 0
    try:
        ok = int(final.get("ch_incidents", 0) or 0)
    except Exception:
        ok = 0
    sys.exit(0 if ok > 0 else 1)


if __name__ == "__main__":
    main()
