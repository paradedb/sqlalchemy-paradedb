from __future__ import annotations


def test_vector_diagnostic_sql():
    from unittest.mock import MagicMock
    from paradedb.sqlalchemy import diagnostics

    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    for function in ("vector_info", "vector_config", "vector_estimator_info"):
        getattr(diagnostics, "paradedb_" + function)(engine, "search_idx", "embedding")
        connection.exec_driver_sql.assert_called_with(
            f"SELECT * FROM paradedb.{function}(%s::regclass, %s::text)",
            ("search_idx", "embedding"),
        )
    diagnostics.paradedb_vector_estimator_info(engine, "search_idx", "embedding", [[0.1, 0.2]])
    connection.exec_driver_sql.assert_called_with(
        "SELECT * FROM paradedb.vector_estimator_info(%s::regclass, %s::text, %s::vector[])",
        ("search_idx", "embedding", ["[0.1,0.2]"]),
    )
