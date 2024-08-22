{{
  config(
    materialized = "table",
  )
}}

with source as (
    select distinct
        type1,
        type2
    from {{ ref('stg_pokemon') }}
),

together as (
    select type1 as type
    from source
    union
    select type2 as type
    from source
),

final as (
    select distinct
        md5(type) as id,
        type
    from together
    where type is not null
)

select * from final
