{{ config(materialized='table') }}

select *
from {{ source('amazon_ads', 'sb_ad_report') }}