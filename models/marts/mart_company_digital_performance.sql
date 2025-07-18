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

-- Join bridge with full enhanced traffic data to get all metrics
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
        
        -- Traffic identification
        traffic.site_domain,
        traffic.traffic_year,
        traffic.traffic_month,
        traffic.site_main_category,
        traffic.site_category,
        traffic.site_country,
        
        -- Rankings and basic metrics
        traffic.site_global_rank,
        traffic.site_country_rank,
        traffic.site_main_category_rank,
        traffic.site_category_rank,
        
        -- Platform performance
        traffic.mobile_traffic_ratio,
        traffic.desktop_traffic_ratio,
        traffic.platform_preference,
        traffic.platform_engagement_leader,
        
        -- Quality metrics
        traffic.overall_quality_score,
        traffic.quality_tier,
        
        -- Traffic volumes
        traffic.total_visits,
        traffic.total_unique_visitors,
        traffic.total_page_views,
        traffic.desktop_visits,
        traffic.mobile_visits,
        
        -- Engagement metrics
        traffic.total_pages_per_visit,
        traffic.total_visit_duration_seconds,
        traffic.total_bounce_rate,
        traffic.desktop_bounce_rate,
        traffic.mobile_bounce_rate,
        
        -- Traffic sources (desktop)
        traffic.desktop_direct_visits,
        traffic.desktop_organic_search_visits,
        traffic.desktop_paid_search_visits,
        traffic.desktop_referral_visits,
        traffic.desktop_social_visits,
        traffic.desktop_display_visits,
        traffic.desktop_email_visits,
        
        -- Demographics
        traffic.age_18_24_share,
        traffic.age_25_34_share,
        traffic.age_35_44_share,
        traffic.age_45_54_share,
        traffic.age_55_64_share,
        traffic.age_65_plus_share,
        traffic.male_share,
        traffic.female_share,
        
        -- For deduplication if needed (in case multiple traffic records per company)
        row_number() over (
            partition by bridge.organization_id 
            order by traffic.total_visits desc, traffic.site_global_rank asc nulls last
        ) as rn
        
    from bridge_data as bridge
    inner join enhanced_traffic as traffic
        on bridge.web_traffic_id = traffic.web_traffic_id
),

-- Ensure one row per company (select best traffic record if multiple exist)
deduped_companies as (
    select * from company_traffic_full where rn = 1
),

-- Add calculated fields for comprehensive digital performance insights
final as (
    select
        -- Primary key
        organization_id,
        
        -- Company dimensions
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        is_investor,
        organization_created_at,
        organization_updated_at,
        
        -- Digital presence identification
        site_domain,
        traffic_year,
        traffic_month,
        site_main_category as industry_category,
        site_category as industry_subcategory,
        site_country as traffic_country,
        match_confidence_score,
        
        -- Performance rankings
        site_global_rank,
        site_country_rank,
        site_main_category_rank,
        site_category_rank,
        
        -- Overall web performance scores
        overall_quality_score as digital_performance_score,
        quality_tier as performance_tier,
        
        -- Desktop vs mobile performance ratios
        mobile_traffic_ratio,
        desktop_traffic_ratio,
        platform_preference,
        platform_engagement_leader,
        
        -- Traffic volume metrics
        total_visits,
        total_unique_visitors,
        total_page_views,
        desktop_visits,
        mobile_visits,
        
        -- Engagement quality metrics
        total_pages_per_visit,
        total_visit_duration_seconds,
        total_bounce_rate,
        desktop_bounce_rate,
        mobile_bounce_rate,
        
        -- Traffic source breakdown (calculate percentages)
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
        
        -- Raw traffic source numbers
        desktop_direct_visits,
        desktop_organic_search_visits,
        desktop_paid_search_visits,
        desktop_referral_visits,
        desktop_social_visits,
        desktop_display_visits,
        desktop_email_visits,
        
        -- Demographics - age distribution
        age_18_24_share,
        age_25_34_share,
        age_35_44_share,
        age_45_54_share,
        age_55_64_share,
        age_65_plus_share,
        
        -- Demographics - gender distribution
        male_share,
        female_share,
        
        -- Calculate primary demographic segments for easy analysis
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
        
        -- Meta fields
        current_timestamp() as _mart_created_at
        
    from deduped_companies
)

select * from final