from dagster import Definitions, load_assets_from_modules

from tirana_pipeline.assets import analysis, gtfs, opendata, osm
from tirana_pipeline.resources import get_resources

all_assets = load_assets_from_modules([gtfs, opendata, osm, analysis])

defs = Definitions(
    assets=all_assets,
    resources=get_resources(),
)
