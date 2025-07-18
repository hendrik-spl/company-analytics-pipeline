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

enhanced_organizations as (
    select * from {{ ref('int_organizations_enhanced') }}
),

{# Join all data sources to get complete picture #}
category_company_traffic as (
    select
        {# Category dimensions #}
        traffic.site_main_category,
        
        {# Company and geographic info #}
        bridge.organization_id,
        bridge.organization_name,
        org.standardized_city,
        org.standardized_region,
        org.is_investor,
        
        {# Traffic metrics #}
        traffic.total_visits,
        traffic.total_unique_visitors,
        traffic.total_page_views,
        traffic.desktop_visits,
        traffic.mobile_visits,
        
        {# Platform and quality metrics #}
        traffic.mobile_traffic_ratio,
        traffic.desktop_traffic_ratio,
        traffic.platform_preference,
        traffic.overall_quality_score,
        traffic.quality_tier,
        
        {# Demographics #}
        traffic.age_18_24_share,
        traffic.age_25_34_share,
        traffic.age_35_44_share,
        traffic.age_45_54_share,
        traffic.age_55_64_share,
        traffic.age_65_plus_share,
        traffic.male_share,
        traffic.female_share,
        
        {# Traffic sources #}
        traffic.desktop_direct_visits,
        traffic.desktop_organic_search_visits,
        traffic.desktop_paid_search_visits,
        traffic.desktop_referral_visits,
        traffic.desktop_social_visits
        
    from bridge_data as bridge
    inner join enhanced_traffic as traffic
        on bridge.web_traffic_id = traffic.web_traffic_id
    inner join enhanced_organizations as org
        on bridge.organization_id = org.organization_id
),

{# Calculate category-level aggregations #}
category_aggregations as (
    select
        {# Category hierarchy #}
        site_main_category as main_category,
        
        {# Company metrics #}
        count(distinct organization_id) as total_companies,
        count(distinct case when is_investor = true then organization_id end) as investor_companies,
        count(distinct standardized_city) as unique_cities,
        count(distinct standardized_region) as unique_regions,
        
        {# Traffic aggregations #}
        sum(total_visits) as total_category_visits,
        sum(total_unique_visitors) as total_category_unique_visitors,
        sum(total_page_views) as total_category_page_views,
        sum(desktop_visits) as total_desktop_visits,
        sum(mobile_visits) as total_mobile_visits,
        
        {# Platform performance (weighted averages) #}
        case 
            when sum(total_visits) > 0 
            then sum(mobile_visits) / sum(total_visits)
            else null 
        end as category_mobile_ratio,
        
        case 
            when sum(total_visits) > 0 
            then sum(desktop_visits) / sum(total_visits)
            else null 
        end as category_desktop_ratio,
        
        {# Mode calculations for categorical data #}
        mode(platform_preference) as dominant_platform_preference,
        mode(quality_tier) as dominant_quality_tier,
        
        {# Quality metrics (weighted averages) #}
        case 
            when sum(total_visits) > 0 
            then sum(overall_quality_score * total_visits) / sum(total_visits)
            else null 
        end as category_avg_quality_score,
        
        {# Demographics (weighted averages by traffic) #}
        case 
            when sum(total_visits) > 0 
            then sum(age_18_24_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_18_24_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(age_25_34_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_25_34_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(age_35_44_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_35_44_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(age_45_54_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_45_54_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(age_55_64_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_55_64_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(age_65_plus_share * total_visits) / sum(total_visits)
            else null 
        end as category_age_65_plus_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(male_share * total_visits) / sum(total_visits)
            else null 
        end as category_male_share,
        
        case 
            when sum(total_visits) > 0 
            then sum(female_share * total_visits) / sum(total_visits)
            else null 
        end as category_female_share,
        
        {# Traffic sources (percentages of total desktop traffic) #}
        case 
            when sum(desktop_visits) > 0 
            then sum(desktop_direct_visits) / sum(desktop_visits)
            else null 
        end as category_direct_share,
        
        case 
            when sum(desktop_visits) > 0 
            then sum(desktop_organic_search_visits) / sum(desktop_visits)
            else null 
        end as category_organic_share,
        
        case 
            when sum(desktop_visits) > 0 
            then sum(desktop_paid_search_visits) / sum(desktop_visits)
            else null 
        end as category_paid_share,
        
        case 
            when sum(desktop_visits) > 0 
            then sum(desktop_referral_visits) / sum(desktop_visits)
            else null 
        end as category_referral_share,
        
        case 
            when sum(desktop_visits) > 0 
            then sum(desktop_social_visits) / sum(desktop_visits)
            else null 
        end as category_social_share,
        
        {# Geographic distribution #}
        mode(standardized_region) as dominant_region,
        mode(standardized_city) as dominant_city,
        
        {# Top performers for market concentration #}
        max(total_visits) as largest_company_visits,
        min(total_visits) as smallest_company_visits,
        avg(total_visits) as avg_company_visits

    from category_company_traffic
    group by site_main_category
),

{# Calculate market concentration metrics #}
market_concentration as (
    select
        cct.site_main_category,
        
        {# Calculate traffic concentration using coefficient of variation #}
        case 
            when count(distinct cct.organization_id) > 1 and avg(cct.total_visits) > 0
            then stddev(cct.total_visits) / avg(cct.total_visits)
            else null 
        end as traffic_concentration_cv,
        
        {# Calculate share of top company #}
        case 
            when sum(cct.total_visits) > 0
            then max(cct.total_visits) / sum(cct.total_visits)
            else null 
        end as top_company_traffic_share,
        
        {# Market efficiency metric (companies per unit of traffic) #}
        case 
            when sum(cct.total_visits) > 0
            then count(distinct cct.organization_id) / (sum(cct.total_visits) / 1000000.0)
            else null 
        end as companies_per_million_visits

    from category_company_traffic as cct
    group by cct.site_main_category
),

final as (
    select
        {# Primary dimensions #}
        agg.main_category,
        
        {# Company composition #}
        agg.total_companies,
        agg.investor_companies,
        case 
            when agg.total_companies > 0 
            then agg.investor_companies / agg.total_companies 
            else null 
        end as investor_ratio,
        
        {# Geographic distribution #}
        agg.unique_cities,
        agg.unique_regions,
        agg.dominant_region,
        agg.dominant_city,
        
        {# Traffic performance metrics #}
        agg.total_category_visits,
        agg.total_category_unique_visitors,
        agg.total_category_page_views,
        agg.total_desktop_visits,
        agg.total_mobile_visits,
        
        {# Platform preferences #}
        agg.category_mobile_ratio,
        agg.category_desktop_ratio,
        agg.dominant_platform_preference,
        
        {# Quality metrics #}
        agg.category_avg_quality_score,
        agg.dominant_quality_tier,
        
        {# Market concentration metrics #}
        mc.traffic_concentration_cv,
        mc.top_company_traffic_share,
        mc.companies_per_million_visits,
        
        {# Market concentration classification #}
        case 
            when mc.top_company_traffic_share > 0.7 then 'Highly Concentrated'
            when mc.top_company_traffic_share > 0.4 then 'Moderately Concentrated'
            when mc.top_company_traffic_share > 0.2 then 'Somewhat Concentrated'
            else 'Fragmented'
        end as market_concentration_tier,
        
        case 
            when agg.total_companies >= 10 then 'Large Market'
            when agg.total_companies >= 5 then 'Medium Market'
            when agg.total_companies >= 3 then 'Small Market'
            else 'Niche Market'
        end as market_size_tier,
        
        {# Performance metrics #}
        agg.largest_company_visits,
        agg.smallest_company_visits,
        agg.avg_company_visits,
        
        {# Demographics composition #}
        agg.category_age_18_24_share,
        agg.category_age_25_34_share,
        agg.category_age_35_44_share,
        agg.category_age_45_54_share,
        agg.category_age_55_64_share,
        agg.category_age_65_plus_share,
        agg.category_male_share,
        agg.category_female_share,
        
        {# Primary demographic segments #}
        case 
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_18_24_share, 0) then '18-24'
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_25_34_share, 0) then '25-34'
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_35_44_share, 0) then '35-44'
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_45_54_share, 0) then '45-54'
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_55_64_share, 0) then '55-64'
            when greatest(
                coalesce(agg.category_age_18_24_share, 0),
                coalesce(agg.category_age_25_34_share, 0),
                coalesce(agg.category_age_35_44_share, 0),
                coalesce(agg.category_age_45_54_share, 0),
                coalesce(agg.category_age_55_64_share, 0),
                coalesce(agg.category_age_65_plus_share, 0)
            ) = coalesce(agg.category_age_65_plus_share, 0) then '65+'
            else 'Unknown'
        end as primary_age_segment,
        
        case 
            when coalesce(agg.category_male_share, 0) > coalesce(agg.category_female_share, 0) then 'Male-Leaning'
            when coalesce(agg.category_female_share, 0) > coalesce(agg.category_male_share, 0) then 'Female-Leaning'
            when abs(coalesce(agg.category_male_share, 0) - coalesce(agg.category_female_share, 0)) <= 0.1 then 'Balanced'
            else 'Unknown'
        end as primary_gender_segment,
        
        {# Traffic source composition #}
        agg.category_direct_share,
        agg.category_organic_share,
        agg.category_paid_share,
        agg.category_referral_share,
        agg.category_social_share,
        
        {# Meta fields #}
        current_timestamp() as _mart_created_at
        
    from category_aggregations as agg
    left join market_concentration as mc
        on agg.main_category = mc.site_main_category
    where agg.total_companies >= 1  {# Only include categories with at least one Danish company #}
)

select * from final