{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('cb_data', 'organization_summary') }}
),

cleaned as (
    select
        {# Primary key - using UUID as the unique identifier #}
        uuid as organization_id,
        
        {# String fields - core identifiers and descriptive info #}
        name as organization_name,
        permalink as organization_permalink,
        domain as organization_domain,
        type as organization_type,
        roles as organization_roles,
        short_description as organization_description,
        
        {# Geographic information #}
        city as organization_city,
        region as organization_region,
        state_code as organization_state_code,
        country_code as organization_country_code,
        
        {# URL fields - social media and web presence #}
        homepage_url as organization_homepage_url,
        cb_url as organization_cb_url,
        facebook_url as organization_facebook_url,
        linkedin_url as organization_linkedin_url,
        twitter_url as organization_twitter_url,
        logo_url as organization_logo_url,
        
        {# Timestamp fields - audit trail #}
        created_at as organization_created_at,
        updated_at as organization_updated_at

    from source_data
),

final as (
    select
        {# Primary key #}
        organization_id,
        
        {# Core organization info #}
        organization_name,
        organization_permalink,
        organization_domain,
        organization_type,
        organization_roles,
        organization_description,
        
        {# Geographic fields #}
        organization_city,
        organization_region,
        organization_state_code,
        organization_country_code,
        
        {# URL fields #}
        organization_homepage_url,
        organization_cb_url,
        organization_facebook_url,
        organization_linkedin_url,
        organization_twitter_url,
        organization_logo_url,
        
        {# Audit timestamps #}
        organization_created_at,
        organization_updated_at,
        
        {# Meta fields for data quality #}
        current_timestamp() as _loaded_at

    from cleaned
    where organization_id is not null  {# Ensure we have valid primary keys #}
)

select * from final