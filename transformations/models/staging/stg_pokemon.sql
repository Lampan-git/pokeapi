{{
  config(
    materialized = "table"
  )
}}

with source as (
    select *
    from {{ source('raw', "pokemon") }}
),

exploded as (
    select
        pokemon,
        data #>> '{species, name}' as species,
        (data ->> 'order')::int as order, --dm
        (data ->> 'height')::int as height, --dm
        (data ->> 'weight')::int as weight, --hg
        data -> 'abilities' as abilities,
        data -> 'base_experience' as base_experience,
        data -> 'forms' as forms,
        data -> 'game_indices' as game_indices,
        data #>> '{types, 0, type, name}' as type1,
        data #>> '{types, 1, type, name}' as type2,
        data -> 'stats' as stats
    from source
)

select * from exploded
