{{
    config(
        materialized='table'
    )
}}

with danish_investors as (
    select 
        organization_id,
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        organization_created_at,
        organization_updated_at
    from {{ ref('int_organizations_enhanced') }}
    where is_investor = true
),

digital_performance as (
    select * from {{ ref('mart_company_digital_performance') }}
),

growth_performance as (
    select * from {{ ref('mart_company_digital_performance_timeseries') }}
),

{# Join investors with their digital performance metrics #}
investors_with_digital as (
    select
        inv.organization_id,
        inv.organization_name,
        inv.organization_permalink,
        inv.organization_domain,
        inv.standardized_city,
        inv.standardized_region,
        inv.organization_country_code,
        inv.organization_created_at,
        inv.organization_updated_at,
        
        {# Digital presence indicators #}
        case 
            when dp.organization_id is not null then true
            else false
        end as has_digital_presence,
        
        {# Digital performance metrics - handle nulls #}
        coalesce(dp.site_domain, 'Unknown') as site_domain,
        coalesce(dp.industry_category, 'Unknown') as industry_category,
        coalesce(dp.industry_subcategory, 'Unknown') as industry_subcategory,
        coalesce(dp.period_start, 'Unknown') as period_start,
        coalesce(dp.period_end, 'Unknown') as period_end,
        coalesce(dp.months_available, 0) as months_available,
        
        {# Key traffic metrics for popularity #}
        coalesce(dp.total_visits, 0) as total_visits,
        coalesce(dp.total_unique_visitors, 0) as total_unique_visitors,
        coalesce(dp.total_page_views, 0) as total_page_views,
        
        {# Performance indicators #}
        dp.site_global_rank,
        dp.site_country_rank,
        dp.digital_performance_score,
        coalesce(dp.performance_tier, 'Unknown') as performance_tier,
        coalesce(dp.platform_preference, 'Unknown') as platform_preference,
        
        {# Engagement metrics #}
        dp.total_pages_per_visit,
        dp.total_visit_duration_seconds,
        dp.total_bounce_rate,
        
        {# Demographics #}
        coalesce(dp.primary_age_segment, 'Unknown') as primary_age_segment,
        coalesce(dp.primary_gender_segment, 'Unknown') as primary_gender_segment

    from danish_investors as inv
    left join digital_performance as dp
        on inv.organization_id = dp.organization_id
),

{# Add growth metrics #}
investors_with_growth as (
    select
        iwd.*,
        
        {# Growth indicators #}
        case 
            when gp.organization_id is not null then true
            else false
        end as has_growth_data,
        
        {# Growth metrics - handle nulls #}
        gp.visits_growth_rate,
        gp.visits_monthly_growth_rate,
        gp.visits_absolute_change,
        gp.quality_score_change,
        gp.quality_score_change_rate,
        gp.global_rank_change,
        gp.country_rank_change,
        
        {# Growth classifications #}
        coalesce(gp.growth_category, 'Unknown') as growth_category,
        coalesce(gp.quality_trend, 'Unknown') as quality_trend,
        coalesce(gp.ranking_trend, 'Unknown') as ranking_trend,
        coalesce(gp.performance_momentum, 'Unknown') as performance_momentum,
        
        {# Growth health score #}
        gp.growth_health_score,
        
        {# Peak performance metrics #}
        gp.peak_visits,
        gp.best_global_rank,
        gp.peak_quality_score,
        gp.current_vs_peak_visits_ratio,
        gp.current_vs_peak_quality_ratio

    from investors_with_digital as iwd
    left join growth_performance as gp
        on iwd.organization_id = gp.organization_id
),

{# Calculate popularity and growth rankings #}
ranked_investors as (
    select
        *,
        
        {# Popularity rankings based on traffic volume #}
        case 
            when has_digital_presence = true
            then row_number() over (
                order by total_visits desc, 
                         total_unique_visitors desc
            )
            else null
        end as popularity_rank,
        
        case 
            when has_digital_presence = true
            then ntile(10) over (
                order by total_visits desc, 
                         total_unique_visitors desc
            )
            else null
        end as popularity_decile,
        
        {# Growth rankings for rising stars #}
        case 
            when has_growth_data = true and visits_growth_rate is not null
            then row_number() over (
                order by visits_growth_rate desc, 
                         growth_health_score desc nulls last
            )
            else null
        end as growth_rank,
        
        case 
            when has_growth_data = true and visits_growth_rate is not null
            then ntile(10) over (
                order by visits_growth_rate desc, 
                         growth_health_score desc nulls last
            )
            else null
        end as growth_decile

    from investors_with_growth
),

final as (
    select
        {# Primary key #}
        organization_id,
        
        {# Core investor information #}
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        organization_created_at,
        organization_updated_at,
        
        {# Digital presence status #}
        has_digital_presence,
        has_growth_data,
        site_domain,
        industry_category,
        industry_subcategory,
        period_start,
        period_end,
        months_available,
        
        {# Traffic volume metrics (popularity indicators) #}
        total_visits,
        total_unique_visitors,
        total_page_views,
        
        {# Performance metrics #}
        site_global_rank,
        site_country_rank,
        digital_performance_score,
        performance_tier,
        platform_preference,
        
        {# Engagement quality #}
        total_pages_per_visit,
        total_visit_duration_seconds,
        total_bounce_rate,
        
        {# Growth metrics (rising star indicators) #}
        visits_growth_rate,
        visits_monthly_growth_rate,
        visits_absolute_change,
        quality_score_change,
        quality_score_change_rate,
        global_rank_change,
        country_rank_change,
        
        {# Growth classifications #}
        growth_category,
        quality_trend,
        ranking_trend,
        performance_momentum,
        growth_health_score,
        
        {# Peak performance #}
        peak_visits,
        best_global_rank,
        peak_quality_score,
        current_vs_peak_visits_ratio,
        current_vs_peak_quality_ratio,
        
        {# Rankings for analysis #}
        popularity_rank,
        popularity_decile,
        growth_rank,
        growth_decile,
        
        {# Popularity tier classification #}
        case 
            when popularity_decile = 1 then 'Top Tier'
            when popularity_decile <= 3 then 'High Popularity'
            when popularity_decile <= 6 then 'Medium Popularity'
            when popularity_decile <= 8 then 'Low Popularity'
            when popularity_decile is not null then 'Minimal Popularity'
            else 'No Digital Presence'
        end as popularity_tier,
        
        {# Rising star classification #}
        case 
            when growth_decile = 1 then 'Rising Star'
            when growth_decile <= 3 then 'High Growth'
            when growth_decile <= 6 then 'Moderate Growth'
            when growth_decile <= 8 then 'Slow Growth'
            when growth_decile is not null then 'Declining'
            else 'No Growth Data'
        end as rising_star_tier,
        
        {# Combined classification for top performers #}
        case 
            when popularity_decile <= 2 and growth_decile <= 2 
                then 'Popular & Rising'
            when popularity_decile <= 2 
                then 'Popular & Established'
            when growth_decile <= 2 
                then 'Rising Star'
            when has_digital_presence = true 
                then 'Digital Present'
            else 'Unknown Digital Status'
        end as investor_classification,
        
        {# Demographics #}
        primary_age_segment,
        primary_gender_segment,
        
        {# Meta fields #}
        current_timestamp() as _mart_created_at
        
    from ranked_investors
)

select * from final