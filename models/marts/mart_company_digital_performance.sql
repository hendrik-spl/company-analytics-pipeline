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

{# Join bridge with full enhanced traffic data to get all metrics #}
company_traffic_full as (
    select
        bridge.organization_id,
        bridge.web_traffic_id,
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
        
        {# Traffic identification #}
        traffic.site_domain,
        traffic.traffic_year,
        traffic.traffic_month,
        traffic.site_main_category,
        traffic.site_category,
        traffic.site_country,
        
        {# Rankings and basic metrics #}
        traffic.site_global_rank,
        traffic.site_country_rank,
        traffic.site_main_category_rank,
        traffic.site_category_rank,
        
        {# Platform performance #}
        traffic.mobile_traffic_ratio,
        traffic.desktop_traffic_ratio,
        traffic.platform_preference,
        traffic.platform_engagement_leader,
        
        {# Quality metrics #}
        traffic.overall_quality_score,
        traffic.quality_tier,
        
        {# Traffic volumes #}
        traffic.total_visits,
        traffic.total_unique_visitors,
        traffic.total_page_views,
        traffic.desktop_visits,
        traffic.mobile_visits,
        
        {# Engagement metrics #}
        traffic.total_pages_per_visit,
        traffic.total_visit_duration_seconds,
        traffic.total_bounce_rate,
        traffic.desktop_bounce_rate,
        traffic.mobile_bounce_rate,
        
        {# Traffic sources (desktop) #}
        traffic.desktop_direct_visits,
        traffic.desktop_organic_search_visits,
        traffic.desktop_paid_search_visits,
        traffic.desktop_referral_visits,
        traffic.desktop_social_visits,
        traffic.desktop_display_visits,
        traffic.desktop_email_visits,
        
        {# Demographics #}
        traffic.age_18_24_share,
        traffic.age_25_34_share,
        traffic.age_35_44_share,
        traffic.age_45_54_share,
        traffic.age_55_64_share,
        traffic.age_65_plus_share,
        traffic.male_share,
        traffic.female_share
        
    from bridge_data as bridge
    inner join enhanced_traffic as traffic
        on bridge.web_traffic_id = traffic.web_traffic_id
),

{# Aggregate metrics across all available months per company #}
aggregated_companies as (
    select
        {# Primary key #}
        organization_id,
        
        {# Company dimensions (take any since they are identical across months) #}
        any_value(organization_name) as organization_name,
        any_value(organization_permalink) as organization_permalink,
        any_value(organization_domain) as organization_domain,
        any_value(standardized_city) as standardized_city,
        any_value(standardized_region) as standardized_region,
        any_value(organization_country_code) as organization_country_code,
        any_value(is_investor) as is_investor,
        any_value(organization_created_at) as organization_created_at,
        any_value(organization_updated_at) as organization_updated_at,
        
        {# Digital presence identification (take any consistent values) #}
        any_value(site_domain) as site_domain,
        any_value(site_main_category) as industry_category,
        any_value(site_category) as industry_subcategory,
        any_value(site_country) as traffic_country,
        any_value(match_confidence_score) as match_confidence_score,
        
        {# Time period coverage #}
        min(traffic_year || '-' || lpad(traffic_month, 2, '0')) as period_start,
        max(traffic_year || '-' || lpad(traffic_month, 2, '0')) as period_end,
        count(distinct traffic_year || '-' || lpad(traffic_month, 2, '0')) as months_available,
        
        {# Performance rankings (MIN = best performance achieved) #}
        min(site_global_rank) as site_global_rank,
        min(site_country_rank) as site_country_rank,
        min(site_main_category_rank) as site_main_category_rank,
        min(site_category_rank) as site_category_rank,
        
        {# Overall web performance scores (AVERAGED) #}
        avg(overall_quality_score) as digital_performance_score,
        mode(quality_tier) as performance_tier,
        
        {# Platform performance (MODE for preference, AVERAGED for ratios) #}
        avg(mobile_traffic_ratio) as mobile_traffic_ratio,
        avg(desktop_traffic_ratio) as desktop_traffic_ratio,
        mode(platform_preference) as platform_preference,
        mode(platform_engagement_leader) as platform_engagement_leader,
        
        {# Traffic volume metrics (SUMMED) #}
        sum(total_visits) as total_visits,
        sum(total_unique_visitors) as total_unique_visitors,
        sum(total_page_views) as total_page_views,
        sum(desktop_visits) as desktop_visits,
        sum(mobile_visits) as mobile_visits,
        
        {# Engagement quality metrics (AVERAGED) #}
        avg(total_pages_per_visit) as total_pages_per_visit,
        avg(total_visit_duration_seconds) as total_visit_duration_seconds,
        avg(total_bounce_rate) as total_bounce_rate,
        avg(desktop_bounce_rate) as desktop_bounce_rate,
        avg(mobile_bounce_rate) as mobile_bounce_rate,
        
        {# Traffic source volumes (SUMMED) #}
        sum(desktop_direct_visits) as desktop_direct_visits,
        sum(desktop_organic_search_visits) as desktop_organic_search_visits,
        sum(desktop_paid_search_visits) as desktop_paid_search_visits,
        sum(desktop_referral_visits) as desktop_referral_visits,
        sum(desktop_social_visits) as desktop_social_visits,
        sum(desktop_display_visits) as desktop_display_visits,
        sum(desktop_email_visits) as desktop_email_visits,
        
        {# Demographics (AVERAGED) #}
        avg(age_18_24_share) as age_18_24_share,
        avg(age_25_34_share) as age_25_34_share,
        avg(age_35_44_share) as age_35_44_share,
        avg(age_45_54_share) as age_45_54_share,
        avg(age_55_64_share) as age_55_64_share,
        avg(age_65_plus_share) as age_65_plus_share,
        avg(male_share) as male_share,
        avg(female_share) as female_share

    from company_traffic_full
    group by organization_id
),

{# Add calculated fields for comprehensive digital performance insights #}
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
        period_start,
        period_end,
        months_available,
        industry_category,
        industry_subcategory,
        traffic_country,
        match_confidence_score,
        
        {# Performance rankings #}
        site_global_rank,
        site_country_rank,
        site_main_category_rank,
        site_category_rank,
        
        {# Overall web performance scores #}
        digital_performance_score,
        performance_tier,
        
        {# Platform performance ratios #}
        mobile_traffic_ratio,
        desktop_traffic_ratio,
        platform_preference,
        platform_engagement_leader,
        
        {# Traffic volume metrics #}
        total_visits,
        total_unique_visitors,
        total_page_views,
        desktop_visits,
        mobile_visits,
        
        {# Engagement quality metrics #}
        total_pages_per_visit,
        total_visit_duration_seconds,
        total_bounce_rate,
        desktop_bounce_rate,
        mobile_bounce_rate,
        
        {# Traffic source breakdown (calculate percentages from aggregated volumes) #}
        case 
            when desktop_visits > 0 
            then desktop_direct_visits / desktop_visits 
            else null 
        end as desktop_direct_share,
        
        case 
            when desktop_visits > 0 
            then desktop_organic_search_visits / desktop_visits 
            else null 
        end as desktop_organic_share,
        
        case 
            when desktop_visits > 0 
            then desktop_paid_search_visits / desktop_visits 
            else null 
        end as desktop_paid_share,
        
        case 
            when desktop_visits > 0 
            then desktop_referral_visits / desktop_visits 
            else null 
        end as desktop_referral_share,
        
        case 
            when desktop_visits > 0 
            then desktop_social_visits / desktop_visits 
            else null 
        end as desktop_social_share,
        
        case 
            when desktop_visits > 0 
            then desktop_display_visits / desktop_visits 
            else null 
        end as desktop_display_share,
        
        case 
            when desktop_visits > 0 
            then desktop_email_visits / desktop_visits 
            else null 
        end as desktop_email_share,
        
        {# Raw traffic source numbers #}
        desktop_direct_visits,
        desktop_organic_search_visits,
        desktop_paid_search_visits,
        desktop_referral_visits,
        desktop_social_visits,
        desktop_display_visits,
        desktop_email_visits,
        
        {# Demographics - age distribution #}
        age_18_24_share,
        age_25_34_share,
        age_35_44_share,
        age_45_54_share,
        age_55_64_share,
        age_65_plus_share,
        
        {# Demographics - gender distribution #}
        male_share,
        female_share,
        
        {# Calculate primary demographic segments for easy analysis #}
        case 
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_18_24_share, 0) then '18-24'
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_25_34_share, 0) then '25-34'
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_35_44_share, 0) then '35-44'
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_45_54_share, 0) then '45-54'
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_55_64_share, 0) then '55-64'
            when greatest(
                coalesce(age_18_24_share, 0),
                coalesce(age_25_34_share, 0),
                coalesce(age_35_44_share, 0),
                coalesce(age_45_54_share, 0),
                coalesce(age_55_64_share, 0),
                coalesce(age_65_plus_share, 0)
            ) = coalesce(age_65_plus_share, 0) then '65+'
            else 'Unknown'
        end as primary_age_segment,
        
        case 
            when coalesce(male_share, 0) > coalesce(female_share, 0) then 'Male-Leaning'
            when coalesce(female_share, 0) > coalesce(male_share, 0) then 'Female-Leaning'
            when abs(coalesce(male_share, 0) - coalesce(female_share, 0)) <= 0.1 then 'Balanced'
            else 'Unknown'
        end as primary_gender_segment,
        
        {# Meta fields #}
        current_timestamp() as _mart_created_at
        
    from aggregated_companies
)

select * from final