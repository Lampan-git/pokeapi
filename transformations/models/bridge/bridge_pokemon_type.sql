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

surrogate_key as (
    select
        md5(type1 || coalesce(type2, '')) as two_types_id,
        md5(type1) as type1_id,
        md5(type2) as type2_id
    from source
),

together as (
    select
        two_types_id,
        type1_id as type_id
    from surrogate_key
    union
    select
        two_types_id,
        type2_id as type_id
    from surrogate_key
    where type2_id is not null
)

select * from together
