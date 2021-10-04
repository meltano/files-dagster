import sys
import os
import json
import subprocess
from itertools import chain

import dagster
from dagster import Array, Noneable, Enum, EnumValue, Shape
import dagster_shell


# Dagster is particular about naming conventions
def sanitize_name(name):
  return name.replace(".", "_").replace("-", "_")

project_root = os.getenv("MELTANO_PROJECT_ROOT")

timezone = os.getenv("DAGSTER_SCHEDULE_TIMEZONE")

MeltanoScheduleConfig = Shape({
  'name' : str,
  'extractor': str,
  'loader': str,
  'transform': Enum('TransformMode', [
    EnumValue('skip'),
    EnumValue('run'),
    EnumValue('only'),
  ]),
  'interval': str,
  'start_date': str,
  'env': dict,
  'cron_interval': Noneable(str),
  'last_successful_run_ended_at': Noneable(str),
  'elt_args': Array(str),
})

@dagster.solid(config_schema={'schedule': MeltanoScheduleConfig})
def meltano_elt(context):
  pipeline_config = context.solid_config['schedule']
  output, returncode = dagster_shell.utils.execute(
    f".meltano/run/bin schedule run {pipeline_config['name']}",
    output_logging='STREAM',
    log=context.log,
    env={**os.environ, **pipeline_config['env']},
    cwd=project_root,
  )
  return returncode


def create_pipeline(schedule_config):
  name = sanitize_name(f"meltano_{schedule_config['name']}")
  solid = dagster.configured(meltano_elt, name=f"{name}_elt")({'schedule': schedule_config})
  pipeline = dagster.PipelineDefinition(solid_defs=[solid], name=name)

  if not schedule_config['cron_interval']:
    return [pipeline]

  schedule = dagster.ScheduleDefinition(
    name=f"{name}_schedule",
    cron_schedule=schedule_config['cron_interval'],
    pipeline_name=name,
    execution_timezone=timezone,
  )
  return [pipeline, schedule]

@dagster.repository
def meltano_pipelines():
  result = subprocess.run(
    [".meltano/run/bin", "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
  )
  configs = [create_pipeline(config) for config in json.loads(result.stdout)]
  return list(chain.from_iterable(configs))
