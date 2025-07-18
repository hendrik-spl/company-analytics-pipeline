{{
    config(
        materialized='view'
    )
}}

with base_organizations as (
    select * from {{ ref('stg_cb__organizations') }}
    where organization_country_code = 'DNK'
),

geographic_standardized as (
    select
        *,
        
        {# Standardize Danish city names #}
        case 
            when lower(organization_city) in ('copenhagen', 'københavn', 'kobenhavn', 'cph') 
                then 'Copenhagen'
            when lower(organization_city) in ('aarhus', 'århus') 
                then 'Aarhus'
            when lower(organization_city) in ('odense') 
                then 'Odense'
            when lower(organization_city) in ('aalborg', 'ålborg') 
                then 'Aalborg'
            when lower(organization_city) in ('esbjerg') 
                then 'Esbjerg'
            when lower(organization_city) in ('randers') 
                then 'Randers'
            when lower(organization_city) in ('kolding') 
                then 'Kolding'
            when lower(organization_city) in ('horsens') 
                then 'Horsens'
            when lower(organization_city) in ('vejle') 
                then 'Vejle'
            when lower(organization_city) in ('roskilde') 
                then 'Roskilde'
            when lower(organization_city) in ('herning') 
                then 'Herning'
            when lower(organization_city) in ('helsingør', 'helsingor', 'elsinore') 
                then 'Helsingør'
            when lower(organization_city) in ('silkeborg') 
                then 'Silkeborg'
            when lower(organization_city) in ('næstved', 'naestved') 
                then 'Næstved'
            when lower(organization_city) in ('fredericia') 
                then 'Fredericia'
            when lower(organization_city) in ('viborg') 
                then 'Viborg'
            when lower(organization_city) in ('køge', 'koge') 
                then 'Køge'
            when lower(organization_city) in ('taastrup', 'høje-taastrup', 'hoeje-taastrup') 
                then 'Høje-Taastrup'
            else organization_city
        end as standardized_city,
        
        {# Standardize Danish region names #}
        case 
            when lower(organization_region) in ('capital region', 'capital region of denmark', 'hovedstaden', 'region hovedstaden') 
                then 'Capital Region'
            when lower(organization_region) in ('central denmark region', 'central jutland', 'midtjylland', 'region midtjylland') 
                then 'Central Denmark Region'
            when lower(organization_region) in ('north denmark region', 'north jutland', 'nordjylland', 'region nordjylland') 
                then 'North Denmark Region'
            when lower(organization_region) in ('region of southern denmark', 'southern denmark', 'syddanmark', 'region syddanmark') 
                then 'Region of Southern Denmark'
            when lower(organization_region) in ('zealand', 'sjælland', 'sjaelland', 'region sjælland', 'region sjaelland') 
                then 'Region Zealand'
            else organization_region
        end as standardized_region

    from base_organizations
),

role_parsed as (
    select
        *,
        
        {# Create investor flag by checking roles for investor-related terms #}
        case 
            when lower(organization_roles) like '%investor%' 
            then true
            else false
        end as is_investor,
                
    from geographic_standardized
),

domain_standardized as (
    select
        *,
        
        {# Standardize domain field #}
        case 
            when organization_domain is null or trim(organization_domain) = '' 
                then null
            else 
                lower(
                    regexp_replace(
                        regexp_replace(
                            trim(organization_domain),
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
            when organization_domain is null or trim(organization_domain) = '' 
                then null
            else 
                regexp_replace(
                    lower(
                        regexp_replace(
                            regexp_replace(
                                trim(organization_domain),
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

    from role_parsed
),

final as (
    select
        {# Primary key #}
        organization_id,
        
        {# Core organization info #}
        organization_name,
        organization_permalink,
        organization_type,
        organization_description,
        
        {# Geographic fields - original and standardized #}
        organization_city,
        standardized_city,
        organization_region,
        standardized_region,
        organization_state_code,
        organization_country_code,
        
        {# Domain fields - original and standardized #}
        organization_domain,
        standardized_domain,
        root_domain,
        
        {# Role information - original and parsed #}
        organization_roles,
        is_investor,
        
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
        current_timestamp() as _enhanced_at

    from domain_standardized
)

select * from final