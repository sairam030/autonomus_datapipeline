# Operational Comparison Table

| Aspect | Our Model | Apache Airflow | Prefect | Dagster | Apache NiFi |
|---|---|---|---|---|---|
| **Setup Complexity** | Low (docker compose up) | High (DB + scheduler + webserver) | Medium (server + agent) | Medium (daemon + webserver) | High (Java + ZooKeeper) |
| **Learning Curve** | Low (UI-driven, NL chat) | High (Python DAGs, Jinja) | Medium (Python decorators) | Medium (Python, assets) | Medium (flow-based UI) |
| **Pipeline Creation Time** | Minutes (AI-assisted) | Hours (manual Python) | Hours (manual Python) | Hours (manual Python) | Hours (drag-and-drop) |
| **Schema Evolution Support** | Auto-detect + version | Manual handling | Manual handling | Asset-based tracking | Manual schemas |
| **Code Generation** | AI (Gemini LLM) | None | None | None | None |
| **Data Quality Checks** | Dry-run validation | Custom operators | Custom tasks | Built-in (Expectations) | Custom processors |
| **Real-time Capability** | ✓ (Kafka consumer) | ◐ (polling-based) | ✗ (batch only) | ✗ (batch only) | ✓ (native streaming) |
| **Community & Ecosystem** | Research project | Very Large (mature) | Growing | Growing | Large (enterprise) |
| **License** | Educational | Apache 2.0 | Apache 2.0 / Cloud | Apache 2.0 | Apache 2.0 |