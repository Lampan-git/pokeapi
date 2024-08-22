{{
  config(
    materialized = "table",
  )
}}

with source as (
    select *
    from {{ ref('stg_pokemon') }}
),

stat_exploded as (
    select
        pokemon,
        json_array_elements(source.stats) as stat
    from source
),

fact_with_stats as (
    select
        md5(source.pokemon) as id,
        md5(source.type1 || coalesce(source.type2, '')) as dim_type_id,
        source.pokemon as name,
        source.order as national_index, -- almost true
        source.height,
        source.weight,
        (hp.stat ->> 'base_stat')::int as base_hp,
        (attack.stat ->> 'base_stat')::int as base_attack,
        (defense.stat ->> 'base_stat')::int as base_defense,
        (sa.stat ->> 'base_stat')::int as base_special_attack,
        (sd.stat ->> 'base_stat')::int as base_special_defense,
        (speed.stat ->> 'base_stat')::int as base_speed
    from source
    left join stat_exploded as hp
        on source.pokemon = hp.pokemon
            and hp.stat #>> '{stat, name}' = 'hp'
    left join stat_exploded as attack
        on source.pokemon = attack.pokemon
            and attack.stat #>> '{stat, name}' = 'attack'
    left join stat_exploded as defense
        on source.pokemon = defense.pokemon
            and defense.stat #>> '{stat, name}' = 'defense'
    left join stat_exploded as sa
        on source.pokemon = sa.pokemon
            and sa.stat #>> '{stat, name}' = 'special-attack'
    left join stat_exploded as sd
        on source.pokemon = sd.pokemon
            and sd.stat #>> '{stat, name}' = 'special-defense'
    left join stat_exploded as speed
        on source.pokemon = speed.pokemon
            and speed.stat #>> '{stat, name}' = 'speed'
)

select * from fact_with_stats
