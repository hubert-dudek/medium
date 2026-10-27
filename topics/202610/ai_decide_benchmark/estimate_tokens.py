import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path


MILLION = Decimal(1_000_000)


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() and result >= 0 else None


def first_number(values):
    for value in values:
        if value is not None:
            return number(value)
    return None


def decimal_text(value):
    return format(value, "f") if value is not None else None


def usage_counts(record):
    usage = record.get("usage")
    if not isinstance(usage, dict):
        response = record.get("response")
        usage = response.get("usage") if isinstance(response, dict) else None
    if not isinstance(usage, dict):
        return None
    input_details = usage.get("input_tokens_details") or usage.get("prompt_tokens_details") or {}
    output_details = usage.get("output_tokens_details") or usage.get("completion_tokens_details") or {}
    return {
        "input": first_number([usage.get("input_tokens"), usage.get("prompt_tokens")]),
        "output": first_number([usage.get("output_tokens"), usage.get("completion_tokens")]),
        "reasoning": first_number([usage.get("reasoning_tokens"), output_details.get("reasoning_tokens")]),
        "cache_read": first_number([
            usage.get("cached_input_tokens"), usage.get("cache_read_input_tokens"),
            input_details.get("cached_tokens"),
        ]),
        "cache_write": first_number([
            usage.get("cache_creation_input_tokens"), usage.get("cache_write_input_tokens"),
            input_details.get("cache_write_tokens"),
        ]),
    }


def output_accounting(model):
    if model.startswith("system.ai.gpt-6"):
        return "reasoning_included_in_output"
    if model == "system.ai.gemini-3-8-flash":
        return "reasoning_added_to_completion"
    return "provider_reported_output"


def surcharge_multiplier(rate):
    surcharge = rate.get("surcharge") or {}
    eligible, enabled = surcharge.get("eligible"), surcharge.get("enabled")
    if (eligible is not None and not isinstance(eligible, bool)) or (enabled is not None and not isinstance(enabled, bool)):
        raise ValueError("Surcharge eligible/enabled must be true, false, or null")
    if eligible is False or enabled is False:
        return Decimal(1), Decimal(1), Decimal(1)
    uplift = number(surcharge.get("rate"))
    if uplift is None:
        return None, Decimal(1), None
    if eligible is True and enabled is True:
        value = Decimal(1) + uplift
        return value, value, value
    return None, Decimal(1), Decimal(1) + uplift


def estimate_request(record, rate, excluded=False):
    model = record.get("model", "")
    result = {
        "model": model,
        "case_id": record.get("case_id"),
        "repeat": record.get("repeat"),
        "client_request_id": record.get("client_request_id"),
        "request_status": record.get("status"),
        "http_status": record.get("http_status"),
        "correct": record.get("correct"),
        "output_accounting": output_accounting(model),
        "status": "unknown",
        "base_estimate_usd": None,
        "estimated_cost_usd": None,
        "minimum_estimate_usd": None,
        "maximum_estimate_usd": None,
        "is_billed_cost": False,
    }
    if excluded:
        result["reason"] = "Rejected request; billable usage and zero cost are not inferred"
        return result
    if rate and rate.get("pricing_mode") == "free_beta":
        if not model.startswith("ai_decide") or not rate.get("source_url"):
            raise ValueError("free_beta requires AI_DECIDE and an explicit verified source_url")
        result.update({
            "status": "estimated",
            "basis": "published_free_beta",
            "source_url": rate["source_url"],
            "base_estimate_usd": "0",
            "estimated_cost_usd": "0",
            "minimum_estimate_usd": "0",
            "maximum_estimate_usd": "0",
            "reason": "Published beta inference price only; excludes SQL compute and is not evidence of zero billed usage",
        })
        return result
    if model.startswith("ai_decide"):
        result["reason"] = "AI_DECIDE token usage is unavailable and no verified free-beta rate was supplied"
        return result
    if not rate:
        result["reason"] = "No verified rate supplied for this model"
        return result
    counts = usage_counts(record)
    if counts is None or counts["input"] is None or counts["output"] is None:
        result["reason"] = "Input or output token usage is unavailable"
        return result
    result["reported_tokens"] = {key: decimal_text(value) for key, value in counts.items()}
    input_rate = number(rate.get("input_dbu_per_million"))
    output_rate = number(rate.get("output_dbu_per_million"))
    usd_per_dbu = number(rate.get("usd_per_dbu"))
    if input_rate is None or usd_per_dbu is None:
        result["reason"] = "Required input DBU rate or USD per DBU is unavailable"
        return result
    output_tokens = counts["output"]
    if result["output_accounting"] == "reasoning_added_to_completion":
        if counts["reasoning"] is None:
            result["reason"] = "Gemini reasoning token count is unavailable; completion alone understates usage"
            return result
        output_tokens += counts["reasoning"]
    if output_rate is None and output_tokens != 0:
        result["reason"] = "Output tokens are nonzero but no verified output DBU rate is supplied"
        return result
    cache_read = counts["cache_read"] or Decimal(0)
    cache_write = counts["cache_write"] or Decimal(0)
    accounting = rate.get("input_cache_accounting")
    if accounting is None:
        accounting = "unknown" if ".claude-" in model else "included"
    if accounting not in {"included", "excluded", "unknown"}:
        raise ValueError("input_cache_accounting must be included, excluded, or unknown")
    if accounting == "unknown" and (cache_read or cache_write):
        result["reason"] = "Nonzero cache usage has unknown inclusion in the reported input count"
        return result
    input_tokens = counts["input"]
    if accounting == "included":
        input_tokens -= cache_read + cache_write
        if input_tokens < 0:
            result["reason"] = "Cache counts exceed the input count"
            return result
    cache_read_rate = number(rate.get("cache_read_dbu_per_million"))
    cache_write_rate = number(rate.get("cache_write_dbu_per_million"))
    if cache_write and cache_write_rate is None:
        result["reason"] = "Cache write usage is nonzero but no verified cache write rate is supplied"
        return result
    if cache_read_rate is None:
        cache_read_rate = input_rate
    if cache_write_rate is None:
        cache_write_rate = input_rate
    input_dbus = (input_tokens * input_rate + cache_read * cache_read_rate + cache_write * cache_write_rate) / MILLION
    output_dbus = output_tokens * output_rate / MILLION if output_rate is not None else Decimal(0)
    base = (input_dbus + output_dbus) * usd_per_dbu
    multiplier, minimum, maximum = surcharge_multiplier(rate)
    result.update({
        "status": "estimated" if multiplier is not None else "unknown_surcharge",
        "base_estimate_usd": decimal_text(base),
        "estimated_cost_usd": decimal_text(base * multiplier) if multiplier is not None else None,
        "minimum_estimate_usd": decimal_text(base * minimum),
        "maximum_estimate_usd": decimal_text(base * maximum) if maximum is not None else None,
        "base_input_dbu": decimal_text(input_dbus),
        "base_output_dbu": decimal_text(output_dbus),
        "billable_output_tokens_for_estimate": decimal_text(output_tokens),
        "output_rate_basis": "supplied_verified_rate" if output_rate is not None else "not_published_zero_output_contribution",
        "input_cache_accounting": accounting,
        "cache_read_discount_applied": bool(cache_read and cache_read_rate < input_rate),
        "unreported_cache_discount_assumed": False,
        "surcharge": rate.get("surcharge"),
        "rates_used": {key: rate.get(key) for key in (
            "input_dbu_per_million", "output_dbu_per_million", "cache_read_dbu_per_million",
            "cache_write_dbu_per_million", "usd_per_dbu",
        )},
    })
    if multiplier is None:
        result["reason"] = "Regional-processing surcharge applicability or enabled state is unknown"
    return result


def sum_complete(rows, field):
    values = [number(row.get(field)) for row in rows]
    return sum(values, Decimal(0)) if rows and all(value is not None for value in values) else None


def summarize(records, estimates, normalize=True):
    attempts = len(records)
    total = sum_complete(estimates, "estimated_cost_usd")
    base = sum_complete(estimates, "base_estimate_usd")
    minimum = sum_complete(estimates, "minimum_estimate_usd")
    maximum = sum_complete(estimates, "maximum_estimate_usd")
    known = [number(item["estimated_cost_usd"]) for item in estimates if item["estimated_cost_usd"] is not None]
    return {
        "attempts": attempts,
        "request_status_counts": dict(Counter(item.get("status", "unknown") for item in records)),
        "unique_posts": len({item.get("case_id") for item in records}),
        "correct": sum(item.get("correct") is True for item in records),
        "estimate_status_counts": dict(Counter(item["status"] for item in estimates)),
        "priced_attempts": len(known),
        "unknown_cost_attempts": attempts - len(known),
        "base_estimate_usd": decimal_text(base),
        "raw_used_cost_all_attempts_usd": decimal_text(total),
        "known_estimated_subtotal_usd": decimal_text(sum(known, Decimal(0))) if known else None,
        "minimum_estimate_usd": decimal_text(minimum),
        "maximum_estimate_usd": decimal_text(maximum),
        "normalized_base_per_five_posts_usd": decimal_text(base * Decimal(5) / attempts) if normalize and attempts and base is not None else None,
        "normalized_cost_per_five_posts_usd": decimal_text(total * Decimal(5) / attempts) if normalize and attempts and total is not None else None,
        "normalized_minimum_per_five_posts_usd": decimal_text(minimum * Decimal(5) / attempts) if normalize and attempts and minimum is not None else None,
        "normalized_maximum_per_five_posts_usd": decimal_text(maximum * Decimal(5) / attempts) if normalize and attempts and maximum is not None else None,
        "normalization": "All attempt costs divided by attempts, multiplied by five; includes invalid outputs and is not cost per five correct decisions" if normalize else None,
        "is_billed_cost": False,
    }


def estimate_sql_compute_scenario(sql, scenario):
    duration = number(scenario.get("duration_seconds"))
    dbu_per_hour = number(scenario.get("dbu_per_hour"))
    usd_per_dbu = number(scenario.get("usd_per_dbu"))
    ready = all(value is not None for value in (duration, dbu_per_hour, usd_per_dbu))
    cost = duration / Decimal(3600) * dbu_per_hour * usd_per_dbu if ready else None
    return {
        "status": "hypothetical_estimate" if ready else "unknown",
        "basis": "Hypothetical total compute duration multiplied by supplied warehouse DBU/hour and USD/DBU rates",
        "duration_seconds": decimal_text(duration),
        "dbu_per_hour": decimal_text(dbu_per_hour),
        "usd_per_dbu": decimal_text(usd_per_dbu),
        "warehouse_size": scenario.get("warehouse_size"),
        "source_urls": scenario.get("source_urls", []),
        "estimated_compute_cost_usd": decimal_text(cost),
        "statement_id": sql.get("statement_id"),
        "observed_decision_count": len(sql.get("rows", [])),
        "auto_stop_idle_timeout_is_not_total_billed_duration": True,
        "actual_billed_duration_unknown": True,
        "included_in_token_cost_totals": False,
        "is_billed_cost": False,
    }


def estimate_manifest(manifest, rates):
    model_rates = rates.get("models", {})
    if not isinstance(model_rates, dict):
        raise ValueError("Rates models must be an object keyed by exact manifest model name")
    groups = {name: [] for name in ("measured", "calibration", "sql_ai_decide", "rejected_http_400", "other_rejected")}
    for field, default_group in (("requests", "measured"), ("calibration_requests", "calibration"), ("rejected_requests", "other_rejected")):
        for record in manifest.get(field, []):
            group = "rejected_http_400" if record.get("http_status") == 400 else default_group
            groups[group].append(record)
    sql = manifest.get("ai_decide_sql", {})
    if sql.get("status") == "SUCCEEDED":
        for row in sql.get("rows", []):
            groups["sql_ai_decide"].append({
                "model": "ai_decide_sql", "case_id": row.get("case_id"), "repeat": 0,
                "client_request_id": sql.get("statement_id"),
                "status": "ok" if row.get("label") in {"approved", "refused"} else "invalid_output",
                "correct": row.get("correct"),
            })
    report = {
        "run_id": manifest.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rate_as_of": rates.get("as_of"),
        "source_urls": rates.get("source_urls", []),
        "rate_metadata": {key: value for key, value in rates.items() if key != "models"},
        "currency": "USD",
        "is_billed_cost": False,
        "token_cost_totals_include_sql_compute_or_gateway_fees": False,
        "basis": "Token estimates use response counters and supplied verified Databricks rates; explicit free_beta entries use published product pricing. Reconcile estimates against billing later",
        "groups": {},
    }
    all_records, all_estimates = [], []
    for name, records in groups.items():
        estimates = [estimate_request(record, model_rates.get(record.get("model")), excluded=name in {"rejected_http_400", "other_rejected"}) for record in records]
        models = {}
        for model in dict.fromkeys(record.get("model") for record in records):
            selected_records = [record for record in records if record.get("model") == model]
            selected_estimates = [record for record in estimates if record.get("model") == model]
            models[model] = summarize(selected_records, selected_estimates, normalize=name in {"measured", "calibration", "sql_ai_decide"})
        report["groups"][name] = {
            "models": models,
            "summary": summarize(records, estimates, normalize=name in {"measured", "calibration", "sql_ai_decide"}),
            "requests": estimates,
            "attempt_unit": "post_decisions_in_one_sql_statement" if name == "sql_ai_decide" else "HTTP_requests",
        }
        all_records.extend(records)
        all_estimates.extend(estimates)
    report["all_attempts_including_calibration_and_rejected"] = summarize(all_records, all_estimates, normalize=False)
    scenario = rates.get("sql_compute_scenario")
    if scenario and sql.get("status") == "SUCCEEDED" and sql.get("rows"):
        report["sql_compute_scenario"] = estimate_sql_compute_scenario(sql, scenario)
    return report


def private_path(path):
    resolved = path.expanduser().resolve()
    repository = Path(__file__).resolve().parents[3]
    if resolved.is_relative_to(repository):
        raise ValueError("Manifests, verified rates, and output must be outside the code repository")
    return resolved


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--rates", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest_path, rates_path, output = map(private_path, (args.manifest, args.rates, args.output))
    if output in {manifest_path, rates_path}:
        raise ValueError("Output must not overwrite the input manifest or rates")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    rates = json.loads(rates_path.read_text(encoding="utf-8-sig"))
    report = estimate_manifest(manifest, rates)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"run_id": report["run_id"], "output": str(output), "summary": report["all_attempts_including_calibration_and_rejected"]}, indent=2))


if __name__ == "__main__":
    main()
