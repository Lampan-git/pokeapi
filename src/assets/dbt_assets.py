from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "transformations").resolve(),
)
#dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def pokemon_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--profiles-dir", "profiles"], context=context).stream()
