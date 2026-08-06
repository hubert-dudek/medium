from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit("PyYAML is required: python -m pip install pyyaml") from exc

ROOT = Path(__file__).resolve().parents[1]
PARAMETER_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
RESOURCE_REF_RE = re.compile(r"\$\{resources\.(pipelines|jobs)\.([A-Za-z0-9_]+)\.id\}")


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = yaml.safe_load(handle)
    assert isinstance(value, dict), f"{path} did not parse to a mapping"
    return value


def resolve_bundle_path(base_yaml: Path, configured_path: str) -> Path:
    return (base_yaml.parent / configured_path).resolve()


def iter_tasks(job_spec: dict[str, Any]) -> list[dict[str, Any]]:
    tasks = job_spec.get("tasks", [])
    assert isinstance(tasks, list), "Job tasks must be a list"
    return tasks


def assert_job_resource_references(
    jobs: dict[str, Any], pipelines: dict[str, Any]
) -> None:
    for job_key, job_spec in jobs.items():
        for task in iter_tasks(job_spec):
            serialized = json.dumps(task)
            for resource_type, resource_key in RESOURCE_REF_RE.findall(serialized):
                available = pipelines if resource_type == "pipelines" else jobs
                assert resource_key in available, (
                    f"{job_key}/{task.get('task_key')} references missing "
                    f"{resource_type[:-1]} {resource_key}"
                )


def assert_job_notebook_paths(jobs_path: Path, jobs: dict[str, Any]) -> None:
    for job_key, job_spec in jobs.items():
        has_job_parameters = bool(job_spec.get("parameters"))
        for task in iter_tasks(job_spec):
            notebook_task = task.get("notebook_task")
            if notebook_task:
                notebook_path = resolve_bundle_path(jobs_path, notebook_task["notebook_path"])
                assert notebook_path.exists(), (
                    f"Missing notebook for {job_key}/{task.get('task_key')}: "
                    f"{notebook_path}"
                )

            # The examples use one clear parameter source per job: job-level
            # pushdown or task base_parameters, never both.
            if has_job_parameters:
                assert not (notebook_task or {}).get("base_parameters"), (
                    f"{job_key}/{task.get('task_key')} mixes job parameters "
                    "with task base_parameters"
                )
                pipeline_task = task.get("pipeline_task", {})
                assert not pipeline_task.get("parameters"), (
                    f"{job_key}/{task.get('task_key')} mixes job parameters "
                    "with pipeline task parameters"
                )


def main() -> int:
    root_config_path = ROOT / "databricks.yml"
    schemas_path = ROOT / "resources" / "schemas.yml"
    pipelines_path = ROOT / "resources" / "pipelines.yml"
    jobs_path = ROOT / "resources" / "jobs.yml"

    main_config = load_yaml(root_config_path)
    assert main_config["bundle"]["name"] == "replace_where_use_cases"
    assert main_config["bundle"]["databricks_cli_version"] == ">= 0.255.0"
    assert main_config.get("include") == ["resources/*.yml"]
    assert main_config.get("experimental", {}).get("skip_name_prefix_for_schema") is True
    assert set(main_config.get("targets", {})) == {"dev"}
    assert main_config["targets"]["dev"].get("mode") == "development"
    assert "presets" not in main_config["targets"]["dev"], (
        "skip_name_prefix_for_schema belongs under top-level experimental"
    )

    schemas = load_yaml(schemas_path)["resources"]["schemas"]
    pipelines = load_yaml(pipelines_path)["resources"]["pipelines"]
    jobs = load_yaml(jobs_path)["resources"]["jobs"]

    assert set(schemas) == {"source_schema", "output_schema"}
    assert len(pipelines) == 6, f"Expected 6 pipelines, got {len(pipelines)}"

    expected_pipeline_keys = {
        "uc01_variant_dimension_mv",
        "uc02_effective_dated_vat_mv",
        "uc03_weather_forecast_st",
        "uc04_parameterized_backfill_st",
        "uc05_open_accounting_period_st",
        "uc06_authoritative_slice_st",
    }
    assert set(pipelines) == expected_pipeline_keys

    mv_count = 0
    st_count = 0
    pipeline_sql_files: set[Path] = set()

    for key, spec in pipelines.items():
        assert spec.get("serverless") is True, f"{key} must use serverless"
        assert spec.get("continuous") is False, f"{key} should be triggered"
        assert spec.get("catalog"), f"{key} must publish to a catalog"
        assert spec.get("schema"), f"{key} must publish to a schema"

        parameters = spec.get("parameters")
        assert isinstance(parameters, dict) and parameters, (
            f"{key} must define pipeline parameters"
        )
        assert {"source_catalog", "source_schema"}.issubset(parameters), (
            f"{key} is missing shared source parameters"
        )
        assert all(isinstance(value, str) for value in parameters.values()), (
            f"{key} parameter defaults must be strings"
        )

        libraries = spec.get("libraries", [])
        assert len(libraries) == 1 and "file" in libraries[0], (
            f"{key} must use exactly one SQL file library"
        )
        sql_path = resolve_bundle_path(pipelines_path, libraries[0]["file"]["path"])
        assert sql_path.exists(), f"Missing source for {key}: {sql_path}"
        assert sql_path.suffix.lower() == ".sql", f"{key} source must be SQL"
        assert sql_path not in pipeline_sql_files, f"SQL source reused by {key}"
        pipeline_sql_files.add(sql_path)

        sql_text = sql_path.read_text(encoding="utf-8")
        sql_upper = sql_text.upper()
        assert "FLOW REPLACE WHERE" in sql_upper, f"{key} lacks REPLACE WHERE"
        assert "BY NAME" in sql_upper, f"{key} lacks BY NAME"

        referenced_parameters = set(PARAMETER_RE.findall(sql_text))
        missing_defaults = referenced_parameters - set(parameters)
        assert not missing_defaults, (
            f"{key} references parameters without defaults: {sorted(missing_defaults)}"
        )

        is_mv = "MATERIALIZED VIEW" in sql_upper
        is_st = "STREAMING TABLE" in sql_upper
        assert is_mv ^ is_st, f"{key} must define exactly one target type"
        if is_mv:
            mv_count += 1
        if is_st:
            st_count += 1
            assert "PIPELINES.RESET.ALLOWED" in sql_upper
            assert "'FALSE'" in sql_upper or '"FALSE"' in sql_upper

    assert mv_count == 2, f"Expected 2 MV examples, got {mv_count}"
    assert st_count == 4, f"Expected 4 ST examples, got {st_count}"
    assert pipelines["uc03_weather_forecast_st"]["parameters"]["weather_history_days"] == "0"

    expected_demo_jobs = {
        "demo_01_variant_dimension",
        "demo_02_effective_dated_vat",
        "demo_03_weather_forecast",
        "demo_04_monitored_backfill",
        "demo_05_open_accounting_period",
        "demo_06_authoritative_slice",
    }
    assert expected_demo_jobs.issubset(jobs), "One or more scenario demo jobs are missing"
    assert {
        "setup_sample_data",
        "initialize_all_examples",
        "monitored_backfill",
        "validate_outputs",
    }.issubset(jobs)

    assert_job_resource_references(jobs, pipelines)
    assert_job_notebook_paths(jobs_path, jobs)

    initialization_job = jobs["initialize_all_examples"]
    initialization_setup_task = initialization_job["tasks"][0]
    assert initialization_setup_task.get("task_key") == "setup"
    assert "notebook_task" in initialization_setup_task, (
        "Initialization must run setup directly so parent job parameters reach it"
    )
    assert "run_job_task" not in initialization_setup_task

    initialization_parameters = {
        item["name"]: str(item["default"])
        for item in initialization_job["parameters"]
    }
    assert initialization_parameters["weather_history_days"] == "3650"
    assert initialization_parameters["recompute_days"] == "3650"
    assert initialization_parameters["restatement_lookback_days"] == "3650"

    mutation_files = sorted((ROOT / "src" / "mutations").glob("*.sql"))
    assert len(mutation_files) == 6, (
        f"Expected 6 mutation notebooks, got {len(mutation_files)}"
    )
    assert all(path.read_text(encoding="utf-8").startswith("-- Databricks notebook source")
               for path in mutation_files)

    monitor_path = ROOT / "src" / "orchestration" / "04_backfill_monitor.py"
    monitor_text = monitor_path.read_text(encoding="utf-8")
    for required in [
        '"parameters"',
        '"backfill_start_date"',
        '"backfill_end_date"',
        '"refresh_selection"',
        "target_flow = qualify_api_dataset",
        '"POST"',
        '"GET"',
        "/api/2.0/pipelines/",
        "pipeline_update_id",
        "claim_token",
    ]:
        assert required in monitor_text, f"Backfill monitor is missing {required}"

    monitor_job_params = jobs["monitored_backfill"]["tasks"][0]["notebook_task"][
        "base_parameters"
    ]
    assert {"output_catalog", "output_schema", "target_dataset"}.issubset(
        monitor_job_params
    )

    payload_path = ROOT / "examples" / "start_update_with_parameters.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert set(payload["parameters"]) >= {
        "source_catalog",
        "source_schema",
        "backfill_start_date",
        "backfill_end_date",
    }
    assert payload["refresh_selection"] == [
        "main.replace_where_demo_out_dev.customer_daily_metrics"
    ]

    forbidden = list(ROOT.rglob("__pycache__")) + list(ROOT.rglob("*.pyc"))
    assert not forbidden, f"Generated Python cache files found: {forbidden}"

    print(
        "Static checks passed: bundle settings, 6 pipelines (2 MV + 4 ST), "
        "SQL parameters, reset protection, jobs, notebooks, monitor, and API payload."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
