import json
import requests
from urllib.request import urlopen, Request
from datetime import datetime
from dagster import (
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
    AssetExecutionContext
)
from sqlmodel import Field, SQLModel, create_engine, Session
from sqlalchemy import JSON, Column

class Pokemon(SQLModel, table=True):
    pokemon: str = Field(primary_key=True)
    data: dict = Field(sa_column=Column(JSON), default={})
    ingested_timestamp: datetime

# Borde använda ENV variabler
class PokemonConfig(Config):
    max_fetch: int = 10
    # Should be in a resource or io manager
    database: str = "postgres"
    hostname: str = "localhost"
    user: str = "postgres"
    password: str = "postgres"

# Borde vara i en grupp, partionera kanske med generation, output schema i raw istället för public
@asset(
    name="pokemon",
    key_prefix="raw"
)
def pokemon(context: AssetExecutionContext, config: PokemonConfig):
    fetch_url = f"https://pokeapi.co/api/v2/pokemon?limit={config.max_fetch}&offset=0"
    req = Request(
        url=fetch_url,
        headers={'User-Agent': 'Mozilla/5.0'}
    )
    with urlopen(req) as response:
        raw_json = response.read().decode('utf-8')
        data = json.loads(raw_json)

    pokemon_list = [] 
    for pokemon in data["results"]:
        poke_req = Request(
            url=pokemon["url"],
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urlopen(poke_req) as poke_response:
            pokemon_json = poke_response.read().decode('utf-8')
            data = json.loads(pokemon_json)
            pokemon_data = json.loads(pokemon_json)
            pokemon_list.append(
                Pokemon(pokemon = pokemon["name"], data = pokemon_data, ingested_timestamp = datetime.now())
            )

    # Should be in a resource or io manager
    engine = create_engine(f"postgresql://{config.user}:{config.password}@{config.hostname}/{config.database}")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        for poke in pokemon_list:
            session.merge(poke)
        session.commit()
        
    return MaterializeResult(asset_key=["raw", "pokemon"], metadata={"rows": len(pokemon_list)})
