# Introduction

This project demonstrates an analyzing Twitter hashtag data and visualize the outcomes of the analysis. The project uses Hadoop MapReduce to process and aggregate hashtag frequencies from a large dataset of tweets. The processed data is then visualized using a Python web application. 

# Overview

### Preprocessing (Preprocess.java)
- Cleans and extracts hashtags from tweet data.
- Uses regex to remove URLs, special characters, and then writes a CSV (id, date, hashtag, freq).
- Map-only job that writes directly to HDFS.

### Daily Hashtag Analysis (com.top_hashtags_daily.HashTagPopAnz.java)
- Reads the preprocessed CSV file.
- Maps each line by extracting date and hashtag, aggregating frequencies in the reducer.
- Produces daily totals for each hashtag.

### Monthly Hashtag Analysis (com.top_hashtags_monthly.HashTagPopAnz.java)
- Similar to daily analysis but extracts (year-month) from the date field before counting.
- Aggregates monthly totals for each hashtag.

### Visualization App (visualize-data/main.py)

- Built with [Plotly Dash](https://plotly.com/dash/) for interactive hashtag frequency trends.  
- Loads daily and monthly summary CSVs from Hadoop output, caching data to improve performance.  
- Provides date range pickers to filter and visualize different time periods.  
- Generates line plots with color-coded hashtags, automatically updating on user input.  
- Useful for quickly identifying popular or emerging hashtags over time.

# Prerequisites & Setup

- Java and Maven environment (reference `pom.xml`).
- Docker environment for the Hadoop cluster (`docker-compose.yml`).
- Optional Python environment for `main.py`.

# Data Flow

1. Preprocess stage (`com.preprocess.Preprocess`)
2. Daily hashtag aggregation (`com.top_hashtags_daily.HashTagPopAnz`)
3. Monthly hashtag aggregation (`com.top_hashtags_monthly.HashTagPopAnz`)
4. Visualization (`visualize-data/main.py`)

# Further Development

- Incorporate additional map-reduce steps as needed.
- Expand Python visualization plots or dashboards.
- Integrate with other data processing pipelines if required.