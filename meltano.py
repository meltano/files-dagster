import os
import json
import subprocess

import dagster
from dagster import (
    Array,
    Noneable,
    Enum,
    EnumValue,
)

import dagster_shell


def parent_dir(steps):
    p = os.path.dirname(os.path.realpath(__file__))
    for _ in range(steps):
        p = os.path.dirname(p)
    return p


# Dagster is particular about naming conventions
def sanitize_name(name):
    return name.replace(".", "_").replace("-", "_")


def execute(*args, **kwargs):
    output, returncode = dagster_shell.utils.execute(*args, **kwargs)
    if returncode:
        raise dagster.Failure(f"Shell command execution failed: {returncode}")

    return output


project_root = os.getenv("MELTANO_PROJECT_ROOT", parent_dir(2))

timezone = os.getenv("DAGSTER_SCHEDULE_TIMEZONE")


@dagster.solid(
    config_schema={
        "elt_args": Array(str),
        "env": Noneable(dict),
        "models": Noneable(str),
    }
)
def meltano_elt(context):
    """Run `meltano elt` with the provided args."""
    args = " ".join(context.solid_config["elt_args"])
    cmd = f".meltano/run/bin elt {args}"

    env = {}
    if context.solid_config["models"]:
        env["DBT_MODELS"] = context.solid_config["models"]
    env.update(os.environ)
    if context.solid_config["env"]:
        env.update(context.solid_config["env"])

    execute(
        cmd,
        output_logging="STREAM",
        log=context.log,
        env=env,
        cwd=project_root,
    )


@dagster.solid(
    config_schema={
        "models": str,
        "target": Enum(
            "dbt_target",
            [
                EnumValue("postgres"),
                EnumValue("snowflake"),
            ],
        ),
    }
)
def dbt_snapshot(context, _):
    """Run `dbt snapshot` on the provided model select pattern."""
    models = context.solid_config["models"]
    target = context.solid_config["target"]
    execute(
        f".meltano/run/bin invoke dbt snapshot --target {target} --select {models}",
        output_logging="STREAM",
        log=context.log,
        env={**os.environ},
        cwd=project_root,
    )


@dagster.repository
def meltano_pipelines():
    """Return all the pipelines and schedules for our Meltano project.

    It creates a pipeline to run `meltano elt` and `dbt snapshot`.
    Then it inspects the schedules defined by our project, creating
    a configuration preset and a schedule.
    """
    result = subprocess.run(
        [".meltano/run/bin", "schedule", "list", "--format=json"],
        cwd=project_root,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
    )
    configs = json.loads(result.stdout)

    pipeline_name = "elt_pipeline"
    preset_defs = []
    schedules = []
    for config in configs:
        name = sanitize_name(f"meltano_{config['name']}")

        # Unless otherwise specified, we run transforms and snapshots
        # on the models that match the name of the pipeline.
        # You can change this behavior by setting DBT_MODELS in the env
        # config of the schedule.
        # NOTE: the + syntax here (e.g. "+gitlab+") says also run all parent
        # and child models.
        # See: https://docs.getdbt.com/reference/node-selection/graph-operators
        if "DBT_MODELS" in config["env"]:
            models = config["env"]["DBT_MODELS"]
        else:
            models = f'+{config["name"]}+'

        if "DBT_TARGET" in config["env"]:
            target = config["env"]["DBT_TARGET"]
        else:
            # guess the target from the name of the loader, e.g. target-postgres
            target = config["loader"].replace("target-", "")

        run_config = {
            "solids": {
                "meltano_elt": {
                    "config": {
                        "env": config["env"],
                        "elt_args": config["elt_args"],
                        "models": models,
                    },
                },
                "dbt_snapshot": {
                    "config": {
                        "models": models,
                        "target": target,
                    },
                },
            },
        }
        preset = dagster.PresetDefinition(name=name, run_config=run_config)
        preset_defs.append(preset)

        # for configs without a cron interval, we'll add a preset but not a schedule
        if config["cron_interval"]:
            schedule = dagster.ScheduleDefinition(
                name=f"{name}_schedule",
                cron_schedule=config["cron_interval"],
                pipeline_name=pipeline_name,
                run_config=run_config,
                execution_timezone=timezone,
            )
            schedules.append(schedule)

    @dagster.pipeline(
        name=pipeline_name,
        preset_defs=preset_defs,
    )
    def _elt_pipeline():
        # NOTE: We need snapshot to happen after elt. Since Dagster is built around
        # DAGs, it assumes that tasks can be run in parallel unless their outputs
        # depend on each other. So dbt_snapshot takes an ignored argument.
        # This I consider to be a workaround pending
        # https://gitlab.com/meltano/meltano/-/issues/2301
        # See also:
        # https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs#order-based-dependencies-nothing-dependencies
        dbt_snapshot(meltano_elt())

    return [_elt_pipeline, *schedules]
