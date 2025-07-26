-- models/staging/amazon_ads/stg_amazon_ads__sb_campaign_history.sql
with source as (
  select * from {{ source('amazon_ads', 'sb_campaign_history') }}
)

select * from source
