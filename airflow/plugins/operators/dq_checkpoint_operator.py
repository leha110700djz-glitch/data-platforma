"""
Custom Airflow operator — runs Great Expectations checkpoint and blocks pipeline on failure.
Demonstrates: ООП, наследование BaseOperator, типизация.
"""
from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator


class GreatExpectationsCheckpointOperator(BaseOperator):
    """Runs GE checkpoint; raises AirflowFailException if validation fails."""

    template_fields = ("checkpoint_name", "suite_name", "conn_id")

    def __init__(
        self,
        *,
        checkpoint_name: str,
        suite_name: str,
        conn_id: str,
        validation_failure_raise: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.checkpoint_name = checkpoint_name
        self.suite_name = suite_name
        self.conn_id = conn_id
        self.validation_failure_raise = validation_failure_raise

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        from great_expectations import get_context
        from great_expectations.checkpoint import CheckpointResult

        ge_context = get_context(context_root_dir="/opt/airflow/dq/great_expectations")
        result: CheckpointResult = ge_context.run_checkpoint(
            checkpoint_name=self.checkpoint_name,
            batch_identifiers={"conn_id": self.conn_id},
        )

        stats = result.get_summary_stats()
        self.log.info("GE checkpoint '%s' results: %s", self.checkpoint_name, stats)

        if not result.success and self.validation_failure_raise:
            failed = [
                eid for eid, er in result.run_results.items()
                if not er.exception_info["raised_exception"] and not er.validation_result.success
            ]
            raise AirflowFailException(
                f"GE checkpoint '{self.checkpoint_name}' FAILED on: {failed}"
            )

        return {"checkpoint": self.checkpoint_name, **stats}
