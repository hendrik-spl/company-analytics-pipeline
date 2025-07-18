{{
    config(
        materialized='table'
    )
}}

with bridge_data as (
    select * from {{ ref('int_company_web_bridge') }}
),

enhanced_traffic as (
    select * from {{ ref('int_web_traffic_enhanced') }}
),

{# Join bridge with full enhanced traffic data to get all metrics with time dimension #}
company_traffic_timeseries as (
    select
        bridge.organization_id,
        bridge.organization_name,
        bridge.organization_permalink,
        bridge.organization_domain,
        bridge.standardized_city,
        bridge.standardized_region,
        bridge.organization_country_code,
        bridge.is_investor,
        bridge.organization_created_at,
        bridge.organization_updated_at,
        bridge.match_confidence_score,
        
        {# Traffic identification with time #}
        traffic.site_domain,
        traffic.traffic_year,
        traffic.traffic_month,
        traffic.site_main_category,
        traffic.site_category,
        traffic.site_country,
        
        {# Key metrics for growth analysis #}
        traffic.total_visits,
        traffic.overall_quality_score,
        traffic.site_global_rank,
        traffic.site_country_rank,
        
        {# Additional context metrics #}
        traffic.total_unique_visitors,
        traffic.total_bounce_rate,
        traffic.platform_preference,
        traffic.quality_tier
        
    from bridge_data as bridge
    inner join enhanced_traffic as traffic
        on bridge.web_traffic_id = traffic.web_traffic_id
),

{# Calculate first and last month metrics for each company #}
time_series_boundaries as (
    select
        organization_id,
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        is_investor,
        organization_created_at,
        organization_updated_at,
        
        {# Digital presence info (take any since consistent) #}
        any_value(site_domain) as site_domain,
        any_value(site_main_category) as industry_category,
        any_value(site_category) as industry_subcategory,
        any_value(site_country) as traffic_country,
        any_value(match_confidence_score) as match_confidence_score,
        
        {# Time period analysis #}
        min(traffic_year || '-' || lpad(traffic_month, 2, '0')) as first_month,
        max(traffic_year || '-' || lpad(traffic_month, 2, '0')) as last_month,
        count(distinct traffic_year || '-' || lpad(traffic_month, 2, '0')) as months_available,
        datediff('month', 
            to_date(min(traffic_year || '-' || lpad(traffic_month, 2, '0')) || '-01', 'YYYY-MM-DD'),
            to_date(max(traffic_year || '-' || lpad(traffic_month, 2, '0')) || '-01', 'YYYY-MM-DD')
        ) + 1 as months_span,
        
        {# First month metrics (earliest available data) #}
        min_by(total_visits, traffic_year || lpad(traffic_month, 2, '0')) as first_month_visits,
        min_by(overall_quality_score, traffic_year || lpad(traffic_month, 2, '0')) as first_month_quality,
        min_by(site_global_rank, traffic_year || lpad(traffic_month, 2, '0')) as first_month_global_rank,
        min_by(site_country_rank, traffic_year || lpad(traffic_month, 2, '0')) as first_month_country_rank,
        min_by(total_unique_visitors, traffic_year || lpad(traffic_month, 2, '0')) as first_month_unique_visitors,
        min_by(total_bounce_rate, traffic_year || lpad(traffic_month, 2, '0')) as first_month_bounce_rate,
        
        {# Last month metrics (most recent available data) #}
        max_by(total_visits, traffic_year || lpad(traffic_month, 2, '0')) as last_month_visits,
        max_by(overall_quality_score, traffic_year || lpad(traffic_month, 2, '0')) as last_month_quality,
        max_by(site_global_rank, traffic_year || lpad(traffic_month, 2, '0')) as last_month_global_rank,
        max_by(site_country_rank, traffic_year || lpad(traffic_month, 2, '0')) as last_month_country_rank,
        max_by(total_unique_visitors, traffic_year || lpad(traffic_month, 2, '0')) as last_month_unique_visitors,
        max_by(total_bounce_rate, traffic_year || lpad(traffic_month, 2, '0')) as last_month_bounce_rate,
        
        {# Peak performance metrics #}
        max(total_visits) as peak_visits,
        min(site_global_rank) as best_global_rank,
        max(overall_quality_score) as peak_quality_score,
        
        {# Average performance across period #}
        avg(total_visits) as avg_visits,
        avg(overall_quality_score) as avg_quality_score,
        mode(platform_preference) as dominant_platform_preference,
        mode(quality_tier) as dominant_quality_tier

    from company_traffic_timeseries
    group by 
        organization_id, organization_name, organization_permalink, organization_domain,
        standardized_city, standardized_region, organization_country_code, is_investor,
        organization_created_at, organization_updated_at
    having count(distinct traffic_year || '-' || lpad(traffic_month, 2, '0')) >= 2  {# Minimum 2 months for growth analysis #}
),

{# Calculate growth metrics and classifications #}
growth_calculations as (
    select
        *,
        
        {# VISITS GROWTH ANALYSIS #}
        case 
            when first_month_visits > 0 and last_month_visits is not null
                then (last_month_visits - first_month_visits) / first_month_visits
            else null
        end as visits_growth_rate,
        
        case 
            when first_month_visits > 0 and last_month_visits is not null and months_span > 1
                then power((last_month_visits / first_month_visits), (1.0 / (months_span - 1))) - 1
            else null
        end as visits_monthly_growth_rate,
        
        last_month_visits - first_month_visits as visits_absolute_change,
        
        {# QUALITY SCORE ANALYSIS #}
        case 
            when first_month_quality is not null and last_month_quality is not null
                then last_month_quality - first_month_quality
            else null
        end as quality_score_change,
        
        case 
            when first_month_quality is not null and last_month_quality is not null
                then (last_month_quality - first_month_quality) / first_month_quality
            else null
        end as quality_score_change_rate,
        
        {# RANKING ANALYSIS (remember: lower rank number = better performance) #}
        case 
            when first_month_global_rank is not null and last_month_global_rank is not null
                then first_month_global_rank - last_month_global_rank  {# Positive = rank improved #}
            else null
        end as global_rank_change,
        
        case 
            when first_month_country_rank is not null and last_month_country_rank is not null
                then first_month_country_rank - last_month_country_rank  {# Positive = rank improved #}
            else null
        end as country_rank_change,
        
        {# PERFORMANCE VS PEAK ANALYSIS #}
        case 
            when last_month_visits > 0 and peak_visits > 0
                then last_month_visits / peak_visits
            else null
        end as current_vs_peak_visits_ratio,
        
        case 
            when last_month_quality is not null and peak_quality_score is not null
                then last_month_quality / peak_quality_score
            else null
        end as current_vs_peak_quality_ratio

    from time_series_boundaries
),

{# Add business classifications for growth patterns #}
final as (
    select
        {# Primary key #}
        organization_id,
        
        {# Company dimensions #}
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        is_investor,
        organization_created_at,
        organization_updated_at,
        
        {# Digital presence identification #}
        site_domain,
        industry_category,
        industry_subcategory,
        traffic_country,
        match_confidence_score,
        
        {# Time period analysis #}
        first_month,
        last_month,
        months_available,
        months_span,
        
        {# Performance benchmarks #}
        dominant_platform_preference,
        dominant_quality_tier,
        avg_visits,
        avg_quality_score,
        
        {# First month baseline #}
        first_month_visits,
        first_month_quality,
        first_month_global_rank,
        first_month_country_rank,
        first_month_unique_visitors,
        first_month_bounce_rate,
        
        {# Last month current state #}
        last_month_visits,
        last_month_quality,
        last_month_global_rank,
        last_month_country_rank,
        last_month_unique_visitors,
        last_month_bounce_rate,
        
        {# Peak performance achieved #}
        peak_visits,
        best_global_rank,
        peak_quality_score,
        
        {# Growth metrics - visits #}
        visits_growth_rate,
        visits_monthly_growth_rate,
        visits_absolute_change,
        
        {# Growth metrics - quality #}
        quality_score_change,
        quality_score_change_rate,
        
        {# Growth metrics - rankings #}
        global_rank_change,
        country_rank_change,
        
        {# Performance vs peak #}
        current_vs_peak_visits_ratio,
        current_vs_peak_quality_ratio,
        
        {# BUSINESS CLASSIFICATIONS #}
        
        {# Overall growth category based on visits #}
        case 
            when visits_growth_rate > 0.5 then 'High Growth'
            when visits_growth_rate > 0.2 then 'Moderate Growth'
            when visits_growth_rate > -0.05 then 'Stable'
            when visits_growth_rate > -0.2 then 'Declining'
            when visits_growth_rate is not null then 'Significant Decline'
            else 'Insufficient Data'
        end as growth_category,
        
        {# Quality improvement classification #}
        case 
            when quality_score_change > 0.1 then 'Quality Improving'
            when quality_score_change > -0.05 then 'Quality Stable'
            when quality_score_change is not null then 'Quality Declining'
            else 'Quality Unknown'
        end as quality_trend,
        
        {# Ranking trend (positive change = rank improved) #}
        case 
            when global_rank_change > 100 then 'Rank Significantly Improved'
            when global_rank_change > 20 then 'Rank Improved'
            when global_rank_change > -20 then 'Rank Stable'
            when global_rank_change > -100 then 'Rank Declined'
            when global_rank_change is not null then 'Rank Significantly Declined'
            else 'Rank Unknown'
        end as ranking_trend,
        
        {# Momentum classification #}
        case 
            when current_vs_peak_visits_ratio > 0.9 then 'Near Peak Performance'
            when current_vs_peak_visits_ratio > 0.7 then 'Strong Performance'
            when current_vs_peak_visits_ratio > 0.5 then 'Moderate Performance'
            when current_vs_peak_visits_ratio is not null then 'Below Peak Performance'
            else 'Performance Unknown'
        end as performance_momentum,
        
        {# Combined growth health score (0-100) #}
        case 
            when visits_growth_rate is not null and quality_score_change is not null
                then least(100, greatest(0, 
                    50 + {# Base score #}
                    (visits_growth_rate * 30) + {# Growth component (can add/subtract 30) #}
                    (quality_score_change * 20) {# Quality component (can add/subtract 20) #}
                ))
            when visits_growth_rate is not null
                then least(100, greatest(0, 50 + (visits_growth_rate * 40)))
            when quality_score_change is not null
                then least(100, greatest(0, 50 + (quality_score_change * 30)))
            else null
        end as growth_health_score,
        
        {# Meta fields #}
        current_timestamp() as _mart_created_at
        
    from growth_calculations
)

select * from final