# 🚀 Company Digital Performance Analytics Pipeline

## 📋 Project Overview

This project demonstrates a modern data engineering pipeline that combines company intelligence from Crunchbase with digital performance metrics from SimilarWeb to provide actionable insights into Nordic companies' online presence and growth patterns.

## 📊 Data Sources

| Source | Description | Key Fields |
|--------|-------------|------------|
| **Crunchbase** | Company profiles and metadata | Company name, domain, location, roles, descriptions |
| **SimilarWeb** | Website traffic and engagement metrics | Traffic volumes, demographics, engagement, rankings |

### 🎯 Focus Area
Primary analysis focuses on **Nordic companies** (Denmark, Sweden, Norway, Finland, Iceland) to provide regionally-relevant insights while maintaining manageable scope.

## 🏛️ dbt Project Structure

```
models/
├── staging/           # 🧱 Raw data standardization
│   ├── crunchbase/    
│   │   ├── _crunchbase__sources.yml
│   │   ├── _crunchbase__models.yml
│   │   └── stg_cb__organizations.sql
│   └── similarweb/
│       ├── _similarweb__sources.yml
│       ├── _similarweb__models.yml
│       └── stg_sw__web_traffic.sql
│
├── intermediate/      # ⚙️ Business logic and data preparation
│   ├── _intermediate__models.yml
│   ├── int_organizations_enhanced.sql
│   ├── int_web_traffic_enhanced.sql
│   └── int_company_web_bridge.sql
│
└── marts/            # 🎯 Business-ready analytics tables
    ├── _marts__models.yml
    ├── mart_company_digital_performance.sql
    ├── mart_company_digital_performance_timeseries.sql
    └── mart_category_insights.sql
```

## 🔄 Data Flow & Transformations

### 1️⃣ **Staging Layer** - Data Standardization
- **`stg_cb__organizations`**: Standardizes Crunchbase company data with consistent naming
- **`stg_sw__web_traffic`**: Processes SimilarWeb traffic data with surrogate keys and validation

### 2️⃣ **Intermediate Layer** - Business Logic
- **`int_organizations_enhanced`**: 
  - Geographic standardization (Copenhagen/København → Copenhagen)
  - Domain cleaning and standardization
  - Investor role detection
  - Nordic country filtering

- **`int_web_traffic_enhanced`**: 
  - Platform performance analysis (mobile vs desktop)
  - Traffic quality scoring (0-1 scale based on bounce rate, pages/visit, duration)
  - Quality tier classification (High/Medium/Low/Unknown)
  - **Incremental processing** for performance optimization

- **`int_company_web_bridge`**: 
  - Domain-based matching between companies and traffic data
  - Match confidence scoring
  - Data lineage preservation

### 3️⃣ **Marts Layer** - Analytics-Ready Tables

#### 🏢 `mart_company_digital_performance`
**Purpose**: Single source of truth for company digital performance
- Aggregated metrics across all available months
- Traffic volumes, engagement metrics, demographic breakdowns
- Performance rankings and quality scores
- Platform preferences and traffic sources

#### 📈 `mart_company_digital_performance_timeseries`
**Purpose**: Growth analysis and trend identification
- First-to-last month growth calculations
- Peak performance tracking
- Growth categorization (High Growth, Moderate, Stable, Declining)
- Performance momentum analysis

#### 🏭 `mart_category_insights`
**Purpose**: Industry-level market intelligence
- Market concentration analysis
- Demographic composition by industry
- Geographic distribution patterns
- Platform preferences by sector