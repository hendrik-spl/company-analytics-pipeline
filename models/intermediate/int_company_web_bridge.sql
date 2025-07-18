{{
    config(
        materialized='table'
    )
}}

with organizations_with_domains as (
    select 
        organization_id,
        organization_name,
        organization_permalink,
        standardized_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        is_investor,
        organization_created_at,
        organization_updated_at
    from {{ ref('int_organizations_enhanced') }}
    where standardized_domain is not null
        and trim(standardized_domain) != ''
        and lower(trim(standardized_domain)) not in (
            'linkedin.com', 'facebook.com', 'twitter.com', 
            'gmail.com', 'yahoo.com', 'hotmail.com'
        )  {# Exclude common non-business domains #}
),

web_traffic_clean as (
    select 
        web_traffic_id,
        cleaned_site_domain,
        traffic_year,
        traffic_month,
        site_main_category,
        site_category,
        site_country,
        site_global_rank,
        total_visits,
        total_unique_visitors,
        overall_quality_score,
        quality_tier,
        platform_preference
    from {{ ref('int_web_traffic_enhanced') }}
    where cleaned_site_domain is not null
        and trim(cleaned_site_domain) != ''
        and total_visits > 0
),

domain_matching as (
    select
        {# Organization information #}
        org.organization_id,
        org.organization_name,
        org.organization_permalink,
        org.standardized_domain as organization_domain,
        org.standardized_city,
        org.standardized_region,
        org.organization_country_code,
        org.is_investor,
        org.organization_created_at,
        org.organization_updated_at,
        
        {# Web traffic information #}
        traffic.web_traffic_id,
        traffic.cleaned_site_domain as traffic_domain,
        traffic.traffic_year,
        traffic.traffic_month,
        traffic.site_main_category,
        traffic.site_category,
        traffic.site_country,
        traffic.site_global_rank,
        traffic.total_visits,
        traffic.total_unique_visitors,
        traffic.overall_quality_score,
        traffic.quality_tier,
        traffic.platform_preference,
        
        {# Match assessment #}
        case 
            when lower(trim(org.standardized_domain)) = lower(trim(traffic.cleaned_site_domain))
                then true
            else false
        end as is_domain_match,
        
        {# Simple confidence score based on exact domain matching #}
        case 
            when lower(trim(org.standardized_domain)) = lower(trim(traffic.cleaned_site_domain))
                then 0.95  {# High confidence for exact domain match #}
            else 0.0
        end as match_confidence_score,
        
        {# Match method for transparency #}
        case 
            when lower(trim(org.standardized_domain)) = lower(trim(traffic.cleaned_site_domain))
                then 'exact_domain_match'
            else 'no_match'
        end as match_method

    from organizations_with_domains as org
    inner join web_traffic_clean as traffic
        on lower(trim(org.standardized_domain)) = lower(trim(traffic.cleaned_site_domain))
),

final as (
    select
        {# Primary key - unique for each org-traffic combination #}
        {{ dbt_utils.generate_surrogate_key(['organization_id', 'web_traffic_id']) }} as company_web_bridge_id,
        
        {# Foreign keys #}
        organization_id,
        web_traffic_id,
        
        {# Organization details #}
        organization_name,
        organization_permalink,
        organization_domain,
        standardized_city,
        standardized_region,
        organization_country_code,
        is_investor,
        organization_created_at,
        organization_updated_at,
        
        {# Web traffic details #}
        traffic_domain,
        traffic_year,
        traffic_month,
        site_main_category,
        site_category,
        site_country as traffic_country,
        site_global_rank,
        total_visits,
        total_unique_visitors,
        overall_quality_score,
        quality_tier,
        platform_preference,
        
        {# Match metadata #}
        match_confidence_score,
        match_method,
        is_domain_match,
        
        {# Metadata #}
        current_timestamp() as _bridged_at

    from domain_matching
    where is_domain_match = true
        and match_confidence_score > 0
)

select * from final