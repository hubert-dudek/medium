# mutators.py
import json
from dataclasses import replace
from pathlib import Path

from databricks.bundles.core import Bundle, Variable, job_mutator, variables
from databricks.bundles.jobs import Job, JobParameterDefinition


@variables
class Variables:
    business_domain: Variable[str]


@job_mutator
def inject_standard_job_parameters(bundle: Bundle, job: Job) -> Job:
    config = json.loads((Path(__file__).parent / "containers.json").read_text())

    domain = bundle.resolve_variable(Variables.business_domain)
    env_cfg = config[bundle.target]
    domain_cfg = env_cfg["areas"][domain]

    # Pull all values from JSON + add target and business_domain
    params = {"target": bundle.target, "business_domain": domain}
    params.update({k: v for k, v in env_cfg.items() if k != "areas"})
    params.update({k: v for k, v in domain_cfg.items()})

    # Existing explicit job parameters win over injected defaults
    existing = {p.name: p.default for p in (job.parameters or [])}
    merged = {**params, **existing}

    return replace(
        job,
        parameters=[
            JobParameterDefinition.from_dict({"name": k, "default": str(v)})
            for k, v in merged.items()
        ],
    )
