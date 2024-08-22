from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from .assets import pokemons, dbt_assets
from .assets.dbt_assets import dbt_project

all_assets = load_assets_from_modules([pokemons, dbt_assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
