"""Non-conversational agent for Unity Catalog naming convention scans."""

from __future__ import annotations

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from mlflow.genai.agent_server import invoke
from pydantic import BaseModel, Field

from .scan_catalogs import (
    adjust_validate_to_output_format,
    load_skill_file,
    parse_target_catalogs,
    read_information_schema,
    save_report,
)

w = WorkspaceClient()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SKILL_PATH = str(
    _PROJECT_ROOT / "skills" / "enterprise-naming-convention" / "SKILL.md"
)

AGENT_MODEL = os.getenv("AGENT_MODEL", "databricks-gpt-5-2")
SCAN_WAREHOUSE_ID = os.getenv("SCAN_WAREHOUSE_ID", "")
REPORT_VOLUME_PATH = os.getenv("REPORT_VOLUME_PATH", "")
TARGET_CATALOGS = os.getenv("TARGET_CATALOGS", "")
SCAN_REPORT_DIR = os.getenv("SCAN_REPORT_DIR", "naming_convention")
SKILL_FILE_PATH = os.getenv("SKILL_FILE_PATH", _DEFAULT_SKILL_PATH)
SKILL_TEXT = load_skill_file(SKILL_FILE_PATH)


class AgentInput(BaseModel):
    catalogs: list[str] | str | None = Field(
        default=None,
        description="Optional catalog list to scan. Defaults to TARGET_CATALOGS.",
    )


class FieldNames(BaseModel):
    schema_: str = Field(..., alias="schema", description="Schema name")
    table: str = Field(..., description="Table name")
    column: str = Field(..., description="Column name")

    model_config = {"populate_by_name": True}


class NamingCheckResult(BaseModel):
    table: str = Field(..., description="Fully qualified table name using catalog.schema.table")
    field_original: FieldNames = Field(..., description="Original schema, table, and column names")
    field_corrected: FieldNames = Field(..., description="Corrected names after applying the naming convention")
    check_result: str = Field(..., description="PASS or a FAIL message with the detected correction details")


class AgentOutput(BaseModel):
    results: list[NamingCheckResult] = Field(..., description="Naming convention scan results")
    report_path: str = Field(..., description="Saved JSON report path in the configured volume")
    scanned_catalogs: list[str] = Field(..., description="Catalogs included in the scan")


@invoke()
async def invoke_handler(data: dict) -> dict:
    """Scan information_schema and return a naming convention report as JSON."""
    input_data = AgentInput(**(data or {}))
    catalogs = parse_target_catalogs(input_data.catalogs or TARGET_CATALOGS)

    raw_rows = read_information_schema(
        workspace_client=w,
        warehouse_id=SCAN_WAREHOUSE_ID,
        catalogs=catalogs,
    )
    results = adjust_validate_to_output_format(raw_rows, SKILL_TEXT)
    report_path = save_report(
        workspace_client=w,
        report_rows=results,
        volume_path=REPORT_VOLUME_PATH,
        report_dir=SCAN_REPORT_DIR,
    )

    output = AgentOutput(
        results=[NamingCheckResult(**row) for row in results],
        report_path=report_path,
        scanned_catalogs=catalogs,
    )
    return output.model_dump()
