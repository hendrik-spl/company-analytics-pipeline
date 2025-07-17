{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('sw_data', 'global_growth') }}
),

cleaned as (
    select
        {# Composite primary key components #}
        site as site_domain,
        year as traffic_year,
        month as traffic_month,
        
        {# Create surrogate key for unique identification #}
        {{ dbt_utils.generate_surrogate_key(['site', 'year', 'month']) }} as web_traffic_id,
        
        {# String fields - categorization #}
        main_category as site_main_category,
        site_category as site_category,
        
        {# Additional source fields #}
        clean_site as cleaned_site_domain,
        category_rank as site_category_rank,
        
        {# Numeric fields - rankings #}
        global_rank as site_global_rank,
        country_rank as site_country_rank,
        main_category_rank as site_main_category_rank,
        country as site_country,
        
        {# Numeric fields - desktop metrics #}
        desktop_estimated_visits as desktop_visits,
        desktop_estimated_unique as desktop_unique_visitors,
        desktop_estimated_page_views as desktop_page_views,
        desktop_pages_per_visit as desktop_pages_per_visit,
        desktop_visits_duration as desktop_visit_duration_seconds,
        desktop_bounce_rate as desktop_bounce_rate,
        desktop_direct_estimated_visits as desktop_direct_visits,
        desktop_organic_search_estimated_visits as desktop_organic_search_visits,
        desktop_paid_search_estimated_visits as desktop_paid_search_visits,
        desktop_referrals_estimated_visits as desktop_referral_visits,
        desktop_social_estimated_visits as desktop_social_visits,
        desktop_display_estimated_visits as desktop_display_visits,
        desktop_mail_estimated_visits as desktop_email_visits,
        
        {# Numeric fields - mobile web metrics #}
        mobileweb_estimated_visits as mobile_visits,
        mobileweb_estimated_unique as mobile_unique_visitors,
        mobileweb_estimated_page_views as mobile_page_views,
        mobileweb_pages_per_visit as mobile_pages_per_visit,
        mobileweb_visits_duration as mobile_visit_duration_seconds,
        mobileweb_bounce_rate as mobile_bounce_rate,
        
        {# Numeric fields - total/combined metrics #}
        total_estimated_visits as total_visits,
        total_estimated_unique as total_unique_visitors,
        total_estimated_pageviews as total_page_views,
        total_pages_per_visit as total_pages_per_visit,
        total_visits_duration as total_visit_duration_seconds,
        total_bounce_rate as total_bounce_rate,
        deduplicated_audience as deduplicated_audience,
        
        {# Numeric fields - demographic data (age) #}
        total_ages_18_to_24_share as age_18_24_share,
        total_ages_25_to_34_share as age_25_34_share,
        total_ages_35_to_44_share as age_35_44_share,
        total_ages_45_to_54_share as age_45_54_share,
        total_ages_55_to_64_share as age_55_64_share,
        total_ages_65_plus_share as age_65_plus_share,
        
        {# Numeric fields - demographic data (gender) #}
        total_males_share as male_share,
        total_females_share as female_share

    from source_data
),

final as (
    select
        {# Primary key #}
        web_traffic_id,
        
        {# Core identifiers #}
        site_domain,
        cleaned_site_domain,
        traffic_year,
        traffic_month,
        
        {# Site categorization #}
        site_main_category,
        site_category,
        site_country,
        
        {# Rankings #}
        site_global_rank,
        site_country_rank,
        site_main_category_rank,
        site_category_rank,
        
        {# Desktop metrics #}
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
        
        {# Mobile metrics #}
        mobile_visits,
        mobile_unique_visitors,
        mobile_page_views,
        mobile_pages_per_visit,
        mobile_visit_duration_seconds,
        mobile_bounce_rate,
        
        {# Total metrics #}
        total_visits,
        total_unique_visitors,
        total_page_views,
        total_pages_per_visit,
        total_visit_duration_seconds,
        total_bounce_rate,
        deduplicated_audience,
        
        {# Demographics - age #}
        age_18_24_share,
        age_25_34_share,
        age_35_44_share,
        age_45_54_share,
        age_55_64_share,
        age_65_plus_share,
        
        {# Demographics - gender #}
        male_share,
        female_share,
        
        {# Meta fields #}
        current_timestamp() as _loaded_at

    from cleaned
    where site_domain is not null
      and traffic_year is not null
      and traffic_month is not null
)

select * from final