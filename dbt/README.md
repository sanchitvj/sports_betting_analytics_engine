# dbt Analytics Pipeline Structure*

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'fontSize': '16px',
    'fontWeight': 'bold',
    'primaryTextColor': '#000000',
    'primaryColor': '#e1f5fe',
    'primaryBorderColor': '#01579b',
    'fontFamily': 'arial'
  }
}}%%
graph LR
    %% Source Layer
    subgraph "Source Layer - Raw Data"
%%        S1[ESPN API]
%%        S2[The Odds API]
%%        S3[Weather API]
%%        S4[News API]
        
        SR1[raw_nba_games]
        SR2[raw_nfl_games]
        SR3[raw_nhl_games]
        SR4[raw_cfb_games]
        SR5[raw_odds]
%%        SR6[raw_weather]
%%        SR7[raw_news]
    end

    %% Staging Layer
    subgraph "Staging Layer - Data Cleaning"
        ST1[stg_nba_games]
        ST2[stg_nfl_games]
        ST3[stg_nhl_games]
        ST4[stg_cfb_games]
        ST5[stg_odds]
%%        ST6[stg_weather]
%%        ST7[stg_news]
    end

    %% Intermediate Layer
    subgraph "Intermediate Layer - Business Logic"
        I1[int_game_stats]
        I2[int_team_metrics]
        I3[int_odds_movement]
        I4[int_betting_metrics]
%%        I5[int_weather_impact]
%%        I6[int_news_sentiment]
    end

    %% Marts Layer - Core
    subgraph "Marts Layer - Core Dimensions"
        D1[dim_teams]
        D2[dim_venues]
        D3[dim_bookmakers]
        D4[dim_players]
%%        D5[dim_dates]
%%        D6[dim_weather_conditions]
    end

    %% Marts Layer - Analytics
    subgraph "Marts Layer - Facts"
        F1[fct_games]
        F2[fct_odds_movements]
        F3[fct_game_stats]
        F4[fct_betting_performance]
%%        F5[fct_weather_measurements]
%%        F6[fct_news_events]
    end

    %% Connections
%%    S1 & S2 & S3 & S4 --> SR1 & SR2 & SR3 & SR4 & SR5 & SR6 & SR7
%%    SR1 & SR2 & SR3 & SR4 --> ST1 & ST2 & ST3 & ST4
    SR1 --> ST1
    SR2 --> ST2
    SR3 --> ST3
    SR4 --> ST4
    SR5 --> ST5
%%    SR6 --> ST6
%%    SR7 --> ST7

    ST1 & ST2 & ST3 & ST4 --> I1 & I2
    ST5 --> I3 & I4
%%    ST6 --> I5
%%    ST7 --> I6

    I1 & I2 --> D1 & D2 & D4
    I3 --> D3
%%    I5 --> D6
    I1 & I2 --> F1 & F3
    I3 & I4 --> F2 & F4
%%    I5 --> F5
%%    I6 --> F6

    %% Styling
    classDef source fill:#e1f5fe,stroke:#01579b
    classDef staging fill:#f3e5f5,stroke:#4a148c
    classDef intermediate fill:#fff3e0,stroke:#e65100
    classDef dims fill:#e8f5e9,stroke:#1b5e20
    classDef facts fill:#fce4ec,stroke:#880e4f

    class S1,S2,S3,S4,SR1,SR2,SR3,SR4,SR5,SR6,SR7 source
    class ST1,ST2,ST3,ST4,ST5,ST6,ST7 staging
    class I1,I2,I3,I4,I5,I6 intermediate
    class D1,D2,D3,D4,D5,D6 dims
    class F1,F2,F3,F4,F5,F6 facts
```

## Data Flow Layers
### 1. Source Layer (External Tables)
- Raw games data from Iceberg
- Raw odds data from Iceberg
- Raw weather data from Iceberg
- Raw news data from Iceberg
### 2. Staging Layer (Basic Transformations)
- Standardized game events
- Normalized odds movements
- Cleaned weather metrics
- Processed news events
### 3. Intermediate Layer (Business Logic)
- Game performance metrics
- Market efficiency calculations
- Weather impact analysis
- News sentiment scores
### 4. Marts Layer (Analytics Ready)
- Core gaming facts
- Betting analytics
- Performance insights
- Cross-domain analysis

## Analytics Models
### 1. Core Analytics
- Team performance tracking
- Player statistics aggregation
- Venue impact analysis
- Season progression metrics
### 2. Betting Analytics
- Market efficiency metrics
- Bookmaker comparison
- Line movement patterns
- Value betting opportunities
### 3. Cross-Domain Analytics
- Weather impact on performance
- News sentiment effect on odds
- Multifactor correlation analysis
- Predictive modeling features