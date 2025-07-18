{{
    config(
        materialized='incremental',
        unique_key='web_traffic_id',
        on_schema_change='fail',
        incremental_strategy='merge'
    )
}}

with base_traffic as (
    select * from {{ ref('stg_sw__web_traffic') }}
    where total_visits > 0  {# Filter out sites with no traffic #}
    
    {# This is the key part - only process new data on incremental runs #}
    {% if is_incremental() %}
        -- Only process data newer than what we already have
        and (traffic_year > (select max(traffic_year) from {{ this }})
             or (traffic_year = (select max(traffic_year) from {{ this }}) 
                 and traffic_month > (select max(traffic_month) from {{ this }} where traffic_year = (select max(traffic_year) from {{ this }}))))
    {% endif %}
),

platform_metrics as (
    select
        *,
        
        {# Mobile vs Desktop ratios - core platform performance metrics #}
        case 
            when (coalesce(desktop_visits, 0) + coalesce(mobile_visits, 0)) > 0
                then coalesce(mobile_visits, 0) / (coalesce(desktop_visits, 0) + coalesce(mobile_visits, 0))
            else null
        end as mobile_traffic_ratio,
        
        case 
            when (coalesce(desktop_visits, 0) + coalesce(mobile_visits, 0)) > 0
                then coalesce(desktop_visits, 0) / (coalesce(desktop_visits, 0) + coalesce(mobile_visits, 0))
            else null
        end as desktop_traffic_ratio,
        
        {# Platform preference classification #}
        case 
            when coalesce(mobile_visits, 0) > coalesce(desktop_visits, 0) * 2
                then 'Mobile-Dominant'
            when coalesce(desktop_visits, 0) > coalesce(mobile_visits, 0) * 2
                then 'Desktop-Dominant'
            when abs(coalesce(mobile_visits, 0) - coalesce(desktop_visits, 0)) <= greatest(coalesce(mobile_visits, 0), coalesce(desktop_visits, 0)) * 0.3
                then 'Balanced'
            else 'Mixed'
        end as platform_preference,
        
        {# Engagement comparison between platforms #}
        case 
            when coalesce(mobile_bounce_rate, 0) < coalesce(desktop_bounce_rate, 0)
                then 'Mobile-Better-Engagement'
            when coalesce(desktop_bounce_rate, 0) < coalesce(mobile_bounce_rate, 0)
                then 'Desktop-Better-Engagement'
            else 'Similar-Engagement'
        end as platform_engagement_leader

    from base_traffic
),

quality_scoring as (
    select
        *,
        
        {# Individual quality components (normalized 0-1) #}
        case 
            when total_bounce_rate is not null and total_bounce_rate >= 0 and total_bounce_rate <= 1
                then 1 - total_bounce_rate  {# Lower bounce rate = higher quality #}
            else null
        end as bounce_quality_score,
        
        case 
            when total_pages_per_visit is not null and total_pages_per_visit > 0
                then least(total_pages_per_visit / 10.0, 1.0)  {# Cap at 10 pages = perfect score #}
            else null
        end as pages_quality_score,
        
        case 
            when total_visit_duration_seconds is not null and total_visit_duration_seconds > 0
                then least(total_visit_duration_seconds / 300.0, 1.0)  {# Cap at 5 minutes = perfect score #}
            else null
        end as duration_quality_score,
        
        case 
            when total_visits > 0 and total_unique_visitors > 0
                then least(total_unique_visitors / total_visits, 1.0)  {# Higher unique ratio = better quality #}
            else null
        end as uniqueness_quality_score

    from platform_metrics
),

quality_calculated as (
    select
        *,
        
        {# Overall traffic quality score (simple average of available components) #}
        case 
            when bounce_quality_score is not null 
                or pages_quality_score is not null 
                or duration_quality_score is not null 
                or uniqueness_quality_score is not null
                then (
                    coalesce(bounce_quality_score, 0) + 
                    coalesce(pages_quality_score, 0) + 
                    coalesce(duration_quality_score, 0) + 
                    coalesce(uniqueness_quality_score, 0)
                ) / (
                    case when bounce_quality_score is not null then 1 else 0 end +
                    case when pages_quality_score is not null then 1 else 0 end +
                    case when duration_quality_score is not null then 1 else 0 end +
                    case when uniqueness_quality_score is not null then 1 else 0 end
                )
            else null
        end as overall_quality_score,
        
        {# Quality tier classification #}
        case 
            when (
                coalesce(bounce_quality_score, 0) + 
                coalesce(pages_quality_score, 0) + 
                coalesce(duration_quality_score, 0) + 
                coalesce(uniqueness_quality_score, 0)
            ) / (
                case when bounce_quality_score is not null then 1 else 0 end +
                case when pages_quality_score is not null then 1 else 0 end +
                case when duration_quality_score is not null then 1 else 0 end +
                case when uniqueness_quality_score is not null then 1 else 0 end
            ) >= 0.8 then 'High-Quality'
            when (
                coalesce(bounce_quality_score, 0) + 
                coalesce(pages_quality_score, 0) + 
                coalesce(duration_quality_score, 0) + 
                coalesce(uniqueness_quality_score, 0)
            ) / (
                case when bounce_quality_score is not null then 1 else 0 end +
                case when pages_quality_score is not null then 1 else 0 end +
                case when duration_quality_score is not null then 1 else 0 end +
                case when uniqueness_quality_score is not null then 1 else 0 end
            ) >= 0.6 then 'Medium-Quality'
            when (
                coalesce(bounce_quality_score, 0) + 
                coalesce(pages_quality_score, 0) + 
                coalesce(duration_quality_score, 0) + 
                coalesce(uniqueness_quality_score, 0)
            ) / (
                case when bounce_quality_score is not null then 1 else 0 end +
                case when pages_quality_score is not null then 1 else 0 end +
                case when duration_quality_score is not null then 1 else 0 end +
                case when uniqueness_quality_score is not null then 1 else 0 end
            ) >= 0.4 then 'Low-Quality'
            else 'Unknown-Quality'
        end as quality_tier

    from quality_scoring
),

domain_standardized as (
    select
        *,
        
        {# Standardize domain field using same logic as organizations #}
        case 
            when site_domain is null or trim(site_domain) = '' 
                then null
            else 
                lower(
                    regexp_replace(
                        regexp_replace(
                            trim(site_domain),
                            '^(https?://)?(www\.)?',  -- Remove protocol and www
                            ''
                        ),
                        '/.*$',  -- Remove path and everything after domain
                        ''
                    )
                )
        end as standardized_domain,
        
        {# Extract root domain (remove subdomains, keep only domain.tld) #}
        case 
            when site_domain is null or trim(site_domain) = '' 
                then null
            else 
                regexp_replace(
                    lower(
                        regexp_replace(
                            regexp_replace(
                                trim(site_domain),
                                '^(https?://)?(www\.)?',  -- Remove protocol and www
                                ''
                            ),
                            '/.*$',  -- Remove path
                            ''
                        )
                    ),
                    '^[^.]*\.',  -- Remove subdomain (everything before first dot)
                    ''
                )
        end as root_domain

    from quality_calculated
),

final as (
    select
        {# Primary key #}
        web_traffic_id,
        
        {# Core identifiers #}
        site_domain,
        cleaned_site_domain,
        standardized_domain,
        root_domain,
        traffic_year,
        traffic_month,
        
        {# Categories (already well standardized) #}
        site_main_category,
        site_category,
        site_country,
        
        {# Rankings #}
        site_global_rank,
        site_country_rank,
        site_main_category_rank,
        site_category_rank,
        
        {# Platform performance metrics #}
        mobile_traffic_ratio,
        desktop_traffic_ratio,
        platform_preference,
        platform_engagement_leader,
        
        {# Traffic quality metrics #}
        overall_quality_score,
        quality_tier,
        bounce_quality_score,
        pages_quality_score,
        duration_quality_score,
        uniqueness_quality_score,
        
        {# Original traffic metrics #}
        desktop_visits,
        desktop_unique_visitors,
        desktop_page_views,
        desktop_pages_per_visit,
        desktop_visit_duration_seconds,
        desktop_bounce_rate,
        desktop_direct_visits,
        desktop_organic_search_visits,
        desktop_paid_search_visits,
        desktop_referral_visits,
        desktop_social_visits,
        desktop_display_visits,
        desktop_email_visits,
        
        mobile_visits,
        mobile_unique_visitors,
        mobile_page_views,
        mobile_pages_per_visit,
        mobile_visit_duration_seconds,
        mobile_bounce_rate,
        
        total_visits,
        total_unique_visitors,
        total_page_views,
        total_pages_per_visit,
        total_visit_duration_seconds,
        total_bounce_rate,
        deduplicated_audience,
        
        {# Demographics #}
        age_18_24_share,
        age_25_34_share,
        age_35_44_share,
        age_45_54_share,
        age_55_64_share,
        age_65_plus_share,
        male_share,
        female_share,
        
        {# Meta fields #}
        current_timestamp() as _enhanced_at

    from domain_standardized
    where site_domain is not null
)

select * from final