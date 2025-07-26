{{ config(enabled=var('ad_reporting__amazon_ads_enabled', True)) }}

with sponsored_product_report as (
    select
        *
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
    where is_most_recent_record = true
),

sponsored_brand_report as (
    select
        'sponsored_brands' as ad_type,
        cast(sc.profile_id as string) as profile_id,
        sc.portfolio_id,
        sc.name as campaign_name,
        sb.campaign_id,
        sb.ad_group_id,
        sb.ad_id,
        sb.date_day,
        sb.clicks,
        sb.cost,
        sb.impressions,
        sb.purchases_30_d,
        sb.sales_30_d,
        null as advertised_asin,
        null as advertised_sku,
        null as campaign_budget_amount,
        null as campaign_budget_currency_code,
        null as campaign_budget_type,
        null as source_relation  -- set if available
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
    from {{ ref('int_amazon_ads__portfolio_history') }}
),

campaigns as (
    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true
),

ad_groups as (
    select *
    from {{ var('ad_group_history') }}
    where is_most_recent_record = true
),

ads as (
    select *
    from {{ var('product_ad_history') }}
    where is_most_recent_record = true
),

fields as (
    select
        report.ad_type,
        report.source_relation,
        report.date_day,
        account_info.account_name,
        account_info.account_id,
        account_info.country_code,
        report.profile_id,
        coalesce(report.portfolio_name, portfolios.portfolio_name) as portfolio_name,
        coalesce(report.portfolio_id, portfolios.portfolio_id) as portfolio_id,
        report.campaign_name,
        report.campaign_id,
        ad_groups.ad_group_name,
        report.ad_group_id,
        report.ad_id,
        ads.serving_status,
        ads.state,
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
            exclude_fields=['purchases_30_d','sales_30_d']) }}

    from report

    left join ads
        on ads.ad_id = report.ad_id
        and ads.source_relation = report.source_relation

    left join ad_groups
        on ad_groups.ad_group_id = report.ad_group_id
        and ad_groups.source_relation = report.source_relation

    left join campaigns
        on campaigns.campaign_id = report.campaign_id
        and campaigns.source_relation = report.source_relation

    left join portfolios
        on portfolios.portfolio_id = coalesce(report.portfolio_id, campaigns.portfolio_id)
        and portfolios.source_relation = report.source_relation

    left join account_info
        on account_info.profile_id = report.profile_id
        and account_info.source_relation = report.source_relation

    {{ dbt_utils.group_by(20) }}
)

select * from fields
