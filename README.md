# graphbridge-neptune

Amazon Neptune graph engine add-on for `dbt-graph-bridge`.

This package registers a `neptune` graph engine through the
`dbt_graph_bridge.graph_engine` entry point group. It sends openCypher queries
to Neptune's HTTPS `/openCypher` endpoint.

## Installation

```bash
pip install -e ../dbt-graph-bridge
pip install -e .
```

## dbt profile

```yaml
neptune_from_clickhouse:
  type: graphbridge
  sql_engine: dbt_adapter
  sql_engine_config:
    adapter: clickhouse
    profile:
      schema: default
      host: your-clickhouse-host
      port: 8443
      user: default
      password: "{{ env_var('CLICKHOUSE_PASSWORD') }}"
      secure: true
      driver: http

  graph_engine: neptune
  graph_scheme: https
  graph_host: your-neptune-endpoint.cluster-xxxxxxxx.ap-northeast-1.neptune.amazonaws.com
  graph_port: 8182
  graph_database: ""
  graph_user: ""
  graph_password: ""
```

The first implementation targets Neptune clusters reachable over HTTPS from
the machine running dbt. IAM-signed requests are not implemented yet.
