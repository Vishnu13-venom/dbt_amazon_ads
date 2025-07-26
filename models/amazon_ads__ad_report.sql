{{ config(enabled=var('ad_reporting__amazon_ads_enabled', True)) }}

with sponsored_product_report as (
     select
        'sponsored_products' as ad_type,
        date as date_day,
        null as portfolio_id,
        null as portfolio_name,
        null as campaign_name,
        campaign_id,
        ad_group_id,
        ad_id,
        null as ad_group_name,
        advertised_asin,
        advertised_sku,
        campaign_budget_amount,
        campaign_budget_currency_code,
        campaign_budget_type,
        cost,
        clicks,
        impressions,
        purchases_30_d,
        sales_30_d

        {{ amazon_ads_persist_pass_through_columns(
            pass_through_variable='amazon_ads__advertised_product_passthrough_metrics',
            identifier='this',
            transform='identity',
            coalesce_with=0,
            exclude_fields=['purchases_30_d','sales_30_d']
        ) }}
    from {{ var('advertised_product_report') }}
),


sb_ad_report as (
    select
        'sponsored_brands' as ad_type,
        cast(report_date as date) as date_day,
        campaign_id,
        ad_group_id,
        ad_id,
        cost,
        clicks,
        impressions,
        attributed_sales_14_d as sales_30_d,
        attributed_conversions_14_d as purchases_30_d
    from {{ var('sb_ad_report') }}
),

sb_campaigns as (
    select *
    from {{ var('sb_campaign_history') }}
),

sponsored_brand_report as (
     select
        'sponsored_brands' as ad_type,
        date_day,
        sc.portfolio_id,
        sc.name as portfolio_name,  -- ✅ Add this line
        null as campaign_name,
        sb.campaign_id,
        sb.ad_group_id,
        sb.ad_id,
        null as ad_group_name,
        null as advertised_asin,
        null as advertised_sku,
        null as campaign_budget_amount,
        null as campaign_budget_currency_code,
        null as campaign_budget_type,
        sb.cost,
        sb.clicks,
        sb.impressions,
        sb.purchases_30_d,
        sb.sales_30_d
    from sb_ad_report sb
    left join sb_campaigns sc
        on sb.campaign_id = sc.id
),

report as (
    select * from sponsored_product_report
    union all
    select * from sponsored_brand_report
),

account_info as (
    select *
    from {{ var('profile') }}
    where _fivetran_deleted = false
),

portfolios as (
    select *
    from {{ source('amazon_ads', 'portfolio_history') }} 
),

campaigns as (
    select *
    from {{ var('campaign_history') }}
),

ad_groups as (
    select *
    from {{ var('ad_group_history') }}
),

ads as (
    select *
    from {{ var('product_ad_history') }}
),

final as (
    select
        report.ad_type,
        report.source_relation,
        report.date_day,
        ai.account_name,
        ai.account_id,
        ai.country_code,
        report.profile_id,
        coalesce(report.portfolio_name, pf.portfolio_name) as portfolio_name,
        coalesce(report.portfolio_id, pf.portfolio_id) as portfolio_id,
        report.campaign_name,
        report.campaign_id,
        coalesce(report.ad_group_name, ag.ad_group_name) as ad_group_name,
        report.ad_group_id,
        report.ad_id,
        coalesce(report.serving_status, pa.serving_status) as serving_status,
        coalesce(report.state, pa.state) as state,
        report.advertised_asin,
        report.advertised_sku,
        report.campaign_budget_amount,
        report.campaign_budget_currency_code,
        report.campaign_budget_type,
        sum(report.cost) as cost,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.purchases_30_d) as purchases_30_d,
        sum(report.sales_30_d) as sales_30_d

        {{ amazon_ads_persist_pass_through_columns(
            pass_through_variable='amazon_ads__advertised_product_passthrough_metrics',
            identifier='report',
            transform='sum',
            coalesce_with=0,
            exclude_fields=['purchases_30_d','sales_30_d']
        ) }}

    from report

    left join ads pa
        on pa.ad_id = report.ad_id
       

    left join ad_groups ag
        on ag.ad_group_id = report.ad_group_id
        

    left join campaigns ca
        on ca.campaign_id = report.campaign_id
        

    left join portfolios pf
        on pf.portfolio_id = coalesce(report.portfolio_id, ca.portfolio_id)
        

    left join account_info ai
        on ai.profile_id = report.profile_id

    {{ dbt_utils.group_by(20) }}
)

select * from final