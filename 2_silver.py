# Databricks notebook source
# DBTITLE 1,Setup & Helper Functions
"""
Imports PySpark SQL functions for use in subsequent transformations. These functions are used for column operations, conditional logic, and string manipulation.
"""
from pyspark.sql.functions import col, when, regexp_extract, regexp_replace, translate, lit, coalesce

from pyspark.sql import DataFrame

from typing import List

# COMMAND ----------

# DBTITLE 1,Tables in bronze layer
# MAGIC %sql
# MAGIC ---SQL cell: Shows all tables in the 'googleads_bronze' database to verify available raw data sources.
# MAGIC SHOW TABLES IN googleads_bronze;

# COMMAND ----------

# DBTITLE 1,Reading tables into DFs
"""
Reads all Delta tables from the bronze layer into Spark DataFrames and displays the first 3 rows of each for inspection.
Each DataFrame represents a different aspect of Google Ads reporting (campaign, keyword, search term, etc.).
"""
# Read Delta tables
core_campaign_df = spark.table("googleads_bronze.core_campaign_performance")  # Core campaign metrics
# display(core_campaign_df.limit(3))  # Display sample rows

search_keyword_df = spark.table("googleads_bronze.search_keyword_performance")  # Keyword-level metrics
# display(search_keyword_df.limit(3))

search_term_df = spark.table("googleads_bronze.search_term_analysis")  # Search term metrics
# display(search_term_df.limit(3))

conversion_df = spark.table("googleads_bronze.conversion_performance")  # Conversion metrics
# display(conversion_df.limit(3))

ad_copy_df = spark.table("googleads_bronze.ad_copy_and_landing_page_performance")  # Ad copy and landing page metrics
# display(ad_copy_df.limit(3))

optimization_device_df = spark.table("googleads_bronze.optimization_device_and_time")  # Device and time metrics
# display(optimization_device_df.limit(3))

audience_gender_df = spark.table("googleads_bronze.audience_gender_performance")  # Gender audience metrics
# display(audience_gender_df.limit(3))

audience_age_df = spark.table("googleads_bronze.audience_age_performance")  # Age audience metrics
# display(audience_age_df.limit(3))

competitive_impression_df = spark.table("googleads_bronze.competitive_impression_share")  # Impression share metrics
# display(competitive_impression_df.limit(3))

network_df = spark.table("googleads_bronze.network_performance")  # Network-level metrics
# display(network_df.limit(3))

display_ad_df = spark.table("googleads_bronze.display_ad_viewability")  # Display ad viewability metrics
# display(display_ad_df.limit(3))

call_lead_generation_df = spark.table("googleads_bronze.call_lead_generation")  # Call lead generation metrics
# display(call_lead_generation_df.limit(3))

geographic_df = spark.table("googleads_bronze.geographic_performance")  # Geographic performance metrics
# display(geographic_df.limit(3))

ads_performance_df = spark.table("googleads_bronze.ads_performance")  # Ad performance metrics
# display(ads_performance_df.limit(3))

ads_data_df = spark.table("googleads_bronze.ads_data")  # Ad data details
# display(ads_data_df.limit(3))

campaign_asset_df = spark.table("googleads_bronze.campaign_asset")  # Campaign asset metrics
# display(campaign_asset_df.limit(3))

ad_group_ad_asset_view_df = spark.table("googleads_bronze.ad_group_ad_asset_view")  # Ad group asset view metrics
# display(ad_group_ad_asset_view_df.limit(3))

asset_df = spark.table("googleads_bronze.asset")  # Asset data
# display(asset_df.limit(3))

conversion_action_df = spark.table("googleads_bronze.conversion_action")  # Conversion action details
# display(conversion_action_df.limit(3))

placement_performance_df = spark.table("googleads_bronze.placement_performance")  # Placement performance metrics
# display(placement_performance_df.limit(3))

group_placement_performance_df = spark.table("googleads_bronze.group_placement_performance")  # Group placement performance
# display(group_placement_performance_df.limit(3))

geo_target_constant_df = spark.table("googleads_bronze.geo_target_constant")  # Geo target constant data
# display(group_placement_performance_df.limit(3))

# COMMAND ----------

"""
Displays the full DataFrame and schema for the core campaign performance data for further inspection.
Useful for understanding the structure and sample data before transformation.
"""
display(core_campaign_df.limit(3))
core_campaign_df.printSchema()

# COMMAND ----------

# DBTITLE 1,core_campaign
# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in core_campaign_df.columns]  # Clean column names for Spark compatibility

# Define lists for handling nulls using the NEW names.
# numeric_cols_to_fill: List of numeric columns to fill with 0 if null
# string_cols_to_fill: List of string columns to fill with 'Unknown' if null
numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks', 'metrics_ctr',
    'metrics_cost_micros', 'metrics_average_cpc'
]
string_cols_to_fill = [
    'campaign_id', 'campaign_name', 'campaign_status',
    'campaign_advertising_channel_type'
]

# --- Step 2: Chain all transformations into a final DataFrame ---
"""
Renames columns, handles nulls, calculates cost in INR, and selects final columns for the core campaign performance DataFrame.
This block prepares the campaign metrics for analytics and reporting.
"""
df_core_campaign = core_campaign_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn(
        "cost_inr",
        col("metrics_cost_micros") / 1000000  # Convert micro currency to INR
    ) \
    .withColumn("cpc_inr",
        when(col("metrics_clicks") > 0, col("cost_inr") / col("metrics_clicks"))
        .otherwise(0)
    ) \
    .select(
        "campaign_id",
        "segments_date",
        "campaign_name",
        "campaign_status",
        "campaign_advertising_channel_type",
        "metrics_impressions",
        "metrics_clicks",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"  # Keep the new cost column
        # "metrics_cost_micros" is now omitted
    )


# --- Step 3: Display the final, cleaned output ---
"""
Displays the schema and sample data for the transformed core campaign DataFrame.
This is a verification step to ensure the transformation is correct.
"""
print("--- Final Schema ---")
df_core_campaign.printSchema()

print("\n--- Final Transformed Data ---")
display(df_core_campaign.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the search keyword performance DataFrame for inspection.
"""
display(search_keyword_df.limit(3))
search_keyword_df.printSchema()

# COMMAND ----------

# DBTITLE 1,search_keyword
# --- Step 1: Define new names and column lists for cleaning ---
"""
Renames columns, handles nulls, calculates metrics (CTR, CPC, cost_inr), and selects final columns for the search keyword performance DataFrame.
"""
new_column_names = [c.replace('.', '_').replace('__', '_') for c in search_keyword_df.columns]

numeric_cols_to_fill = [
    'ad_group_criterion_quality_info_quality_score',
    'metrics_impressions',
    'metrics_clicks',
    'metrics_cost_micros'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_name',
    'ad_group_criterion_keyword_text',
    'ad_group_criterion_keyword_match_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---
df_search_keyword = search_keyword_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cpc_inr",
        when(col("metrics_clicks") > 0, col("cost_inr") / col("metrics_clicks"))
        .otherwise(0)
    ) \
    .select(
        "segments_date",
        "campaign_id",   
        "ad_group_id",   
        "campaign_name",
        "ad_group_name",
        col("ad_group_criterion_keyword_text").alias("keyword"),
        col("ad_group_criterion_keyword_match_type").alias("keyword_match_type"),
        col("ad_group_criterion_quality_info_quality_score").alias("quality_score"),
        "metrics_impressions",
        "metrics_clicks",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"
    )

# --- Step 3: Display the final, cleaned output ---
"""
Displays the schema and sample data for the transformed search keyword DataFrame.
"""
print("--- Final Keyword Schema ---")
df_search_keyword.printSchema()

print("\n--- Final Transformed Keyword Data ---")
display(df_search_keyword.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the search term analysis DataFrame for inspection.
"""
display(search_term_df.limit(3))
search_term_df.printSchema()

# COMMAND ----------

# DBTITLE 1,search_term
# --- Step 1: Define new names and column lists for cleaning ---
"""
Renames columns, handles nulls, extracts criterion_id, calculates metrics, and selects final columns for the search term analysis DataFrame.
"""

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in search_term_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions',
    'metrics_cost_micros',
    'metrics_average_cpc',
    'metrics_ctr'
]

string_cols_to_fill = [
    'campaign_name',
    'ad_group_name',
    'search_term_view_search_term',
    'search_term_view_status',
    'segments_search_term_match_type',
    'segments_keyword_ad_group_criterion',
    'segments_keyword_info_text',
    'segments_keyword_info_match_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---
# This renames, cleans nulls, extracts criterion_id, calculates new metrics, and selects final columns.

df_search_term = search_term_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn(
        "triggering_keyword_criterion_id",
        # Extract criterion_id from resource name
        # Format: "customers/XXX/adGroupCriteria/YYY~ZZZ"
        # Extract the "ZZZ" part (criterion_id after the tilde)
        regexp_extract(
            col("segments_keyword_ad_group_criterion"), r'adGroupCriteria/\d+~(\d+)', 1)
    ) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .select(
        "segments_date",
        "campaign_id",
        "ad_group_id",
        "campaign_name",
        "ad_group_name",
        
        # Search term details
        col("search_term_view_search_term").alias("search_term"),
        col("search_term_view_status").alias("search_term_status"),
        col("segments_search_term_match_type").alias("search_term_match_type"),
        
        # Triggering keyword details
        "triggering_keyword_criterion_id",
        col("segments_keyword_info_text").alias("triggering_keyword_text"),
        col("segments_keyword_info_match_type").alias("triggering_keyword_match_type"),
        
        # Raw metrics
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        
        # Calculated metrics (cost and engagement-focused)
        "cost_inr",
        "cpc_inr",
        "ctr_pct"
        
        # Note: Removed CVR and CPL calculations since conversion data has zeros/nulls
        # Can add back later when conversion tracking is fixed
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Search Term Schema (with Triggering Keyword Attribution) ---")
df_search_term.printSchema()

# print("\n--- Sample Data Showing Search Term → Keyword Mapping ---")
# print("Ordered by cost:")
# display(df_search_term.select(
#     "search_term",
#     "triggering_keyword_text",
#     "triggering_keyword_match_type",
#     "search_term_match_type",
#     "metrics_clicks",
#     "cost_inr",
#     "ctr_pct"
# ).orderBy(col("cost_inr").desc()))

# print("\n--- Verification: Check Keyword Attribution ---")
# print("Unique triggering keywords:")
# display(df_search_term.select("triggering_keyword_text", "triggering_keyword_match_type") \
#     .distinct() \
#     .orderBy("triggering_keyword_text"))

print("\n--- Full Transformed Search Term Data ---")
display(df_search_term.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the conversion performance DataFrame for inspection.
"""
display(conversion_df.limit(3))
conversion_df.printSchema()

# COMMAND ----------

# DBTITLE 1,conversion
# --- Step 1: Define new names and column lists for cleaning (Corrected) ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in conversion_df.columns]

# Define lists for handling nulls using the REAL, cleaned names
numeric_cols_to_fill = [
    'metrics_all_conversions',
    'metrics_all_conversions_value',
    'metrics_conversions',
    'metrics_conversions_value',
    'metrics_value_per_conversion',
    'metrics_value_per_all_conversions'
]
# Corrected: Use the actual column name from your data
string_cols_to_fill = [
    'segments_conversion_action',
    'segments_conversion_action_name', 'segments_conversion_action_category'
]


# --- Step 2: Chain all transformations for the final DataFrame (Corrected) ---
# This renames, cleans nulls, calculates new metrics, and selects final columns.
df_conversion = conversion_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("value_per_all_conversions",
        when(col("metrics_all_conversions") > 0, col("metrics_all_conversions_value") / col("metrics_all_conversions"))
        .otherwise(0)
    ) \
    .withColumn("value_per_conversion",
        when(col("metrics_conversions") > 0, col("metrics_conversions_value") / col("metrics_conversions"))
        .otherwise(0)
    ) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        # Corrected: Select the actual column and give it a clean alias
        col("segments_conversion_action").alias("conversion_action"),
        col("segments_conversion_action_name").alias("conversion_action_name"),
        col("segments_conversion_action_category").alias("conversion_action_category"),
        col("metrics_conversions").alias("conversions"),
        col("metrics_conversions_value").alias("conversions_value"),
        "value_per_conversion",
        col("metrics_all_conversions").alias("all_conversions"),
        col("metrics_all_conversions_value").alias("all_conversions_value"),
        "value_per_all_conversions"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Conversion Schema ---")
df_conversion.printSchema()

print("\n--- Final Transformed Conversion Data ---")
display(df_conversion.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the ad copy and landing page performance DataFrame for inspection.
"""
display(ad_copy_df.limit(3))
ad_copy_df.printSchema()

# COMMAND ----------

# DBTITLE 1,ad_copy and landing_page
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in ad_copy_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_name',
    'ad_group_ad_ad_final_urls'
]


# --- Step 2: Chain all transformations for the final DataFrame (Alternative Method) ---
# This renames, cleans nulls, calculates new metrics, and selects final columns.
df_ad_copy = ad_copy_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .withColumn(
        "domain",
        regexp_extract(col("ad_group_ad_ad_final_urls"), r'https?://([^/]+)', 1)
    ) \
    .select(
        "segments_date",
        "campaign_id",
        "ad_group_id",
        "campaign_name",
        "ad_group_name",
        col("ad_group_ad_ad_id").alias("ad_id"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cvr_pct",
        col("ad_group_ad_ad_final_urls").alias("final_url"),
        "domain"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Ad Copy Schema ---")
df_ad_copy.printSchema()

print("\n--- Final Transformed Ad Copy Data ---")
display(df_ad_copy.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the optimization device and time DataFrame for inspection.
"""
display(optimization_device_df.limit(3))
optimization_device_df.printSchema()

# COMMAND ----------

# DBTITLE 1,optimization_device
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in optimization_device_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'segments_hour',
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions',
    'metrics_cost_micros',
    'metrics_ctr',
    'metrics_average_cpc'
]
string_cols_to_fill = [
    'segments_device',
    'segments_day_of_week'
]

# --- Step 2: Chain all transformations for the final DataFrame ---

df_optimization_device = optimization_device_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .withColumn("time_of_day",
        when((col("segments_hour") >= 6) & (col("segments_hour") <= 11), "Morning")
        .when((col("segments_hour") >= 12) & (col("segments_hour") <= 17), "Afternoon")
        .when((col("segments_hour") >= 18) & (col("segments_hour") <= 23), "Evening")
        .otherwise("Night")
    ) \
    .select(
        "segments_date",
        "campaign_id",
        col("segments_device").alias("device"),
        col("segments_day_of_week").alias("day_of_week"),
        col("segments_hour").alias("hour"),
        "time_of_day",
        "metrics_impressions",
        "metrics_clicks",
        "ctr_pct",
        "metrics_conversions",
        "cvr_pct",
        "cost_inr",
        "cpc_inr"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Optimization Schema ---")
df_optimization_device.printSchema()

print("\n--- Final Transformed Optimization Data ---")
display(df_optimization_device.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the audience gender performance DataFrame for inspection.
"""
display(audience_gender_df.limit(3))
audience_gender_df.printSchema()

# COMMAND ----------

# DBTITLE 1,audience_gender
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in audience_gender_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions',
    'metrics_cost_micros'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_criterion_gender_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---

df_audience_gender = audience_gender_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "ad_group_id",
        "campaign_name",
        col("ad_group_criterion_gender_type").alias("gender"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cvr_pct",
        "cost_inr"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Gender Schema ---")
df_audience_gender.printSchema()

print("\n--- Final Transformed Gender Data ---")
display(df_audience_gender.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the audience age performance DataFrame for inspection.
"""
display(audience_age_df.limit(3))
audience_age_df.printSchema()

# COMMAND ----------

# DBTITLE 1,audience_age
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in audience_age_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions',
    'metrics_cost_micros'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_criterion_age_range_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---

df_audience_age = audience_age_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .withColumn("age_range",
        translate(
            regexp_replace(col("ad_group_criterion_age_range_type"), "AGE_RANGE_", ""),
            "_", "-"
        )
    ) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "ad_group_id",
        "campaign_name",
        "age_range",
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cvr_pct",
        "cost_inr"
    )


# --- Step 3: Display the final, cleaned output ---

print("--- Final Age Schema ---")
df_audience_age.printSchema()

print("\n--- Final Transformed Age Data ---")
display(df_audience_age.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the competitive impression share DataFrame for inspection.
"""
display(competitive_impression_df.limit(3))
competitive_impression_df.printSchema()

# COMMAND ----------

# DBTITLE 1,competitive_impression
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in competitive_impression_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_search_impression_share',
    'metrics_search_top_impression_share',
    'metrics_search_absolute_top_impression_share'
]
string_cols_to_fill = [
    'campaign_name'
]


# --- Step 2: Chain all transformations for the final DataFrame ---

df_competitive_impression = competitive_impression_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("search_impression_share_pct", col("metrics_search_impression_share") * 100) \
    .withColumn("search_top_is_pct", col("metrics_search_top_impression_share") * 100) \
    .withColumn("search_abs_top_is_pct", col("metrics_search_absolute_top_impression_share") * 100) \
    .withColumn("search_lost_is_pct", (1 - col("metrics_search_impression_share")) * 100) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        col("search_impression_share_pct").alias("impression_share_pct"),
        col("search_top_is_pct").alias("top_impression_share_pct"),
        col("search_abs_top_is_pct").alias("abs_top_impression_share_pct"),
        col("search_lost_is_pct").alias("lost_impression_share_pct")
    )


# --- Step 3: Display the final, cleaned output ---

print("--- Final Competitive Impression Schema ---")
df_competitive_impression.printSchema()

print("\n--- Final Transformed Competitive Impression Data ---")
display(df_competitive_impression.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the network performance DataFrame for inspection.
"""
display(network_df.limit(3))
network_df.printSchema()

# COMMAND ----------

# DBTITLE 1,network
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in network_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions'
]
string_cols_to_fill = [
    'campaign_name',
    'segments_ad_network_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---

df_network = network_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        col("segments_ad_network_type").alias("ad_network"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cvr_pct"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Ad Network Schema ---")
df_network.printSchema()

print("\n--- Final Transformed Ad Network Data ---")
display(df_network.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the display ad viewability DataFrame for inspection.
"""
display(display_ad_df.limit(3))
display_ad_df.printSchema()

# COMMAND ----------

# DBTITLE 1,display_ad
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in display_ad_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_active_view_impressions',
    'metrics_active_view_measurability',
    'metrics_active_view_viewability',
    'metrics_active_view_cpm'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_name'
]


# --- Step 2: Chain all transformations for the final DataFrame ---

df_display_ad = display_ad_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("actual_cpm", col("metrics_active_view_cpm") / 1000000) \
    .withColumn("total_cost", (col("metrics_active_view_impressions") / 1000) * col("actual_cpm")) \
    .select(
        "segments_date",
        "campaign_name",
        "ad_group_name",
        "metrics_active_view_impressions",
        col("metrics_active_view_measurability").alias("measurability_pct"),
        col("metrics_active_view_viewability").alias("viewability_pct"),
        col("actual_cpm").alias("cpm"),
        "total_cost"
    )

# --- Step 3: Display the final, cleaned output ---

print("--- Final Display Ad Schema ---")
df_display_ad.printSchema()

print("\n--- Final Transformed Display Ad Data ---")
display(df_display_ad.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the call lead generation DataFrame for inspection.
"""
display(call_lead_generation_df.limit(3))
call_lead_generation_df.printSchema()

# COMMAND ----------

# DBTITLE 1,call_lead_generation
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in call_lead_generation_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_phone_calls',
    'metrics_phone_impressions',
    'metrics_phone_through_rate'
]
string_cols_to_fill = [
    'campaign_name',
    'ad_group_name'
]


# --- Step 2: Chain all transformations for the final DataFrame ---

df_call_lead_generation = call_lead_generation_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("phone_through_rate_pct", col("metrics_phone_through_rate") * 100) \
    .select(
        "segments_date",
        "campaign_name",
        "ad_group_name",
        "metrics_phone_impressions",
        "metrics_phone_calls",
        col("phone_through_rate_pct").alias("phone_through_rate_pct")
    )


# --- Step 3: Display the final, cleaned output ---

print("--- Final Call Lead Schema ---")
df_call_lead_generation.printSchema()

print("\n--- Final Transformed Call Lead Data ---")
display(df_call_lead_generation.limit(10))

# COMMAND ----------

geo_target_constant_df
"""
Displays the first 3 rows and schema of the geo target constant DataFrame for inspection.
"""
display(geo_target_constant_df.limit(3))
geo_target_constant_df.printSchema()

# COMMAND ----------

# DBTITLE 1,geo_target_constant
"""
Geo target constant reference data.
Acts as a lookup table for location IDs found in other tables.
"""
# --- Step 1: Define new names and column lists for cleaning ---
new_column_names = [c.replace('.', '_') for c in geo_target_constant_df.columns]

# We ensure IDs are treated as strings to handle 64-bit precision safety.
numeric_cols_to_fill = [] 

string_cols_to_fill = [
    'geo_target_constant_id',
    'geo_target_constant_name',
    'geo_target_constant_canonical_name',
    'geo_target_constant_country_code',
    'geo_target_constant_target_type'
]

# --- Step 2: Chain all transformations for the final DataFrame ---
df_geo_target_constant = geo_target_constant_df.toDF(*new_column_names) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .select(
        col("geo_target_constant_id").alias("constant_id"),
        col("geo_target_constant_name").alias("name"),
        col("geo_target_constant_canonical_name").alias("canonical_name"),
        col("geo_target_constant_country_code").alias("country_code"),
        col("geo_target_constant_target_type").alias("target_type")
    )

# --- Step 3: Display the final, cleaned output ---
print("--- Final Geo Target Constant Schema ---")
df_geo_target_constant.printSchema()

print("\n--- Final Transformed Geo Target Constant Data ---")
display(df_geo_target_constant.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the geographic performance DataFrame for inspection.
"""
display(geographic_df.limit(3))
geographic_df.printSchema()

# COMMAND ----------

# DBTITLE 1,geographic
# --- Step 1: Define new names and column lists for cleaning ---

# Generate new column names by replacing '.' with '_'
new_column_names = [c.replace('.', '_') for c in geographic_df.columns]

# Define lists for handling nulls using the NEW, cleaned names
numeric_cols_to_fill = [
    'metrics_impressions',
    'metrics_clicks',
    'metrics_conversions',
    'metrics_cost_micros'
]
string_cols_to_fill = [
    'segments_geo_target_city',
    'geographic_view_location_type'
]


# --- Step 2: Chain all transformations for the final DataFrame ---

df_geographic = geographic_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn(
        "cost_inr",
        col("metrics_cost_micros") / 1000000
    ) \
    .withColumn("ctr_pct",
        when(col("metrics_impressions") > 0, (col("metrics_clicks") / col("metrics_impressions")) * 100)
        .otherwise(0)
    ) \
    .withColumn("cvr_pct",
        when(col("metrics_clicks") > 0, (col("metrics_conversions") / col("metrics_clicks")) * 100)
        .otherwise(0)
    ) \
    .withColumn("city_id", regexp_extract(col("segments_geo_target_city"), r'/(\d+)$', 1)) \
    .select(
        "segments_date",
        "campaign_id",
        "city_id",
        col("geographic_view_location_type").alias("location_type"),
        "metrics_impressions",
        "metrics_clicks",
        "ctr_pct",
        "metrics_conversions",
        "cvr_pct",
        "cost_inr"

)

# --- Step 3: Display the final, cleaned output ---

print("--- Final Geographic Schema ---")
df_geographic.printSchema()

print("\n--- Final Transformed Geographic Data ---")
display(df_geographic.limit(10))

# COMMAND ----------

# DBTITLE 1,geographic_enriched
df_geographic_enriched = df_geographic.join(
    df_geo_target_constant,
    df_geographic.city_id == df_geo_target_constant.constant_id,
    "left"
).select(
    df_geographic["*"],
    df_geo_target_constant["name"].alias("city_name"),
    df_geo_target_constant["canonical_name"],
    df_geo_target_constant["country_code"]
)

print("--- Final Schema ---")
df_geographic_enriched.printSchema()

print("\n--- Final Transformed Data ---")
display(df_geographic_enriched.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the ads performance DataFrame for inspection.
"""
display(ads_performance_df.limit(3))
ads_performance_df.printSchema()


# COMMAND ----------

# DBTITLE 1,ads_performance
new_column_names = [c.replace('.', '_') for c in ads_performance_df.columns]

df_ads_performance = ads_performance_df.toDF(*new_column_names)

#  Define null handling lists
numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks',
    'metrics_conversions', 'metrics_cost_micros'
]
string_cols_to_fill = [
    'ad_group_id', 'ad_group_ad_ad_id'
]

#  Apply transformations
df_ads_performance = ads_performance_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .select(
        "ad_group_id",
        "ad_group_ad_ad_id",
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "cost_inr"
    )

print("--- Ads Performance Schema ---")
df_ads_performance.printSchema()
print("\n--- Ads Performance Sample ---")
display(df_ads_performance.limit(10))


# COMMAND ----------

"""
Displays the first 3 rows and schema of the ads data DataFrame for inspection.
"""
display(ads_data_df.limit(3))
ads_data_df.printSchema()

# COMMAND ----------

# DBTITLE 1,ads_data
# Step 1: Rename all columns by replacing '.' with '_'
# This step remains the same and correctly handles all columns.
new_column_names = [c.replace('.', '_') for c in ads_data_df.columns]
df_ads_data = ads_data_df.toDF(*new_column_names)

# Step 2: Define lists for handling null values based on the new schema
# We'll handle string and numeric types separately for correctness.

string_cols_to_fill = [
    'campaign_id', 'campaign_name', 
    'ad_group_id', 'ad_group_name', 
    'ad_group_ad_ad_id', 'ad_group_ad_ad_type', 'ad_group_ad_status', 
    'ad_group_ad_ad_final_urls', 
    'ad_group_ad_ad_responsive_search_ad_headlines', 
    'ad_group_ad_ad_responsive_search_ad_descriptions', 
    'ad_group_ad_ad_expanded_text_ad_headline_part1', 
    'ad_group_ad_ad_expanded_text_ad_headline_part2', 
    'ad_group_ad_ad_expanded_text_ad_headline_part3', 
    'ad_group_ad_ad_expanded_text_ad_description',
    'ad_group_ad_ad_expanded_text_ad_description2',
    'ad_group_ad_ad_responsive_display_ad_long_headline', 
    'ad_group_ad_ad_responsive_display_ad_headlines',  
    'ad_group_ad_ad_responsive_display_ad_descriptions', 
    'ad_group_ad_ad_image_ad_image_url',
    'ad_group_ad_ad_image_ad_mime_type'
    ##deprecated columns from googleadsv23
    #'ad_group_ad_ad_call_ad_business_name', 
    #'ad_group_ad_ad_call_ad_country_code', 
    #'ad_group_ad_ad_call_ad_phone_number', 
    #'ad_group_ad_ad_call_ad_headline1', 
    #'ad_group_ad_ad_call_ad_headline2', 
    #'ad_group_ad_ad_call_ad_description1', 
    #'ad_group_ad_ad_call_ad_description2', 
]

# Step 3: Apply transformations
# Fill nulls and keep all columns (the .select() is removed).

df_ads_data = ads_data_df.toDF(*new_column_names) \
    .fillna('Unknown', subset=string_cols_to_fill)

# --- Verification ---
print("--- Ads Data Schema ---")
df_ads_data.printSchema()
print("\n--- Ads Data Sample ---")
display(df_ads_data.limit(10))

# COMMAND ----------

for a in df_ads_data.columns:
    print(a)

# COMMAND ----------

"""
Displays the first 3 rows and schema of the campaign asset DataFrame for inspection.
"""
display(campaign_asset_df.limit(3))
campaign_asset_df.printSchema()

# COMMAND ----------

# DBTITLE 1,campaign_asset
"""
Campaign-level asset performance
"""
new_column_names = [c.replace('.', '_') for c in campaign_asset_df.columns]

numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
    'metrics_conversions', 'metrics_ctr', 'metrics_average_cpc'
]
string_cols_to_fill = [
    'campaign_name', 'campaign_status', 'asset_type',
    'campaign_asset_status', 'segments_device'
]

df_campaign_asset = campaign_asset_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct", col("metrics_ctr") * 100) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        "campaign_status",
        "asset_id",
        "asset_type",
        col("campaign_asset_status").alias("asset_status"),
        col("segments_device").alias("device"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"
    )

print("--- Campaign Asset Schema ---")
df_campaign_asset.printSchema()
display(df_campaign_asset.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the ad group ad asset DataFrame for inspection.
"""
display(ad_group_ad_asset_view_df.limit(3))
ad_group_ad_asset_view_df.printSchema()

# COMMAND ----------

# DBTITLE 1,ad_group_ad_asset_view
"""
Ad group ad-level asset performance
"""
new_column_names = [c.replace('.', '_') for c in ad_group_ad_asset_view_df.columns]

numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
    'metrics_conversions', 'metrics_ctr', 'metrics_average_cpc'
]
string_cols_to_fill = [
    'campaign_name', 'ad_group_name', 'ad_group_ad_asset_view_asset',
    'ad_group_ad_asset_view_field_type', 'segments_device'
]

df_ad_group_ad_asset_view = ad_group_ad_asset_view_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct", col("metrics_ctr") * 100) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        "ad_group_id",
        "ad_group_name",
        col("ad_group_ad_asset_view_asset").alias("asset"),
        col("ad_group_ad_asset_view_field_type").alias("field_type"),
        col("segments_device").alias("device"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"
    )

print("--- Ad Group Ad Asset View Schema ---")
df_ad_group_ad_asset_view.printSchema()
display(df_ad_group_ad_asset_view.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the asset DataFrame for inspection.
"""
display(asset_df.limit(3))
asset_df.printSchema()

# COMMAND ----------

# DBTITLE 1,asset
"""
Asset master data (text, images, sitelinks, callouts, structured snippets)
"""
new_column_names = [c.replace('.', '_') for c in asset_df.columns]

string_cols_to_fill = [
    'asset_type', 'asset_source', 'asset_text_asset_text',
    'asset_image_asset_full_size_url', 'asset_sitelink_asset_link_text',
    'asset_callout_asset_callout_text', 'asset_structured_snippet_asset_header',
    'asset_structured_snippet_asset_values'
]

df_asset = asset_df.toDF(*new_column_names) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .select(
        "asset_id",
        "asset_type",
        "asset_source",
        col("asset_text_asset_text").alias("text_asset_text"),
        col("asset_image_asset_full_size_url").alias("image_url"),
        col("asset_sitelink_asset_link_text").alias("sitelink_text"),
        col("asset_callout_asset_callout_text").alias("callout_text"),
        col("asset_structured_snippet_asset_header").alias("snippet_header"),
        col("asset_structured_snippet_asset_values").alias("snippet_values")
    )

print("--- Asset Schema ---")
df_asset.printSchema()
display(df_asset.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the conversion action DataFrame for inspection.
"""
display(conversion_action_df.limit(3))
conversion_action_df.printSchema()

# COMMAND ----------

# DBTITLE 1,conversion_action
"""
Conversion action configuration data
"""
new_column_names = [c.replace('.', '_') for c in conversion_action_df.columns]

numeric_cols_to_fill = [
    'conversion_action_click_through_lookback_window_days',
    'conversion_action_view_through_lookback_window_days'
]
string_cols_to_fill = [
    'conversion_action_name', 'conversion_action_status', 'conversion_action_category',
    'conversion_action_primary_for_goal', 'conversion_action_counting_type',
    'conversion_action_attribution_model_settings_attribution_model',
    'conversion_action_include_in_conversions_metric'
]

df_conversion_action = conversion_action_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .select(
        "conversion_action_id",
        col("conversion_action_name").alias("name"),
        col("conversion_action_status").alias("status"),
        col("conversion_action_category").alias("category"),
        col("conversion_action_primary_for_goal").alias("primary_for_goal"),
        col("conversion_action_counting_type").alias("counting_type"),
        col("conversion_action_attribution_model_settings_attribution_model").alias("attribution_model"),
        col("conversion_action_click_through_lookback_window_days").alias("click_through_lookback_window_days"),
        col("conversion_action_view_through_lookback_window_days").alias("view_through_lookback_window_days"),
        col("conversion_action_include_in_conversions_metric").alias("include_in_conversions")
    )

print("--- Conversion Action Schema ---")
df_conversion_action.printSchema()
display(df_conversion_action.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the placement performance DataFrame for inspection.
"""
display(placement_performance_df.limit(3))
placement_performance_df.printSchema()

# COMMAND ----------

# DBTITLE 1,placement_performance
"""
Detailed placement performance (specific URLs/apps/videos)
"""
new_column_names = [c.replace('.', '_') for c in placement_performance_df.columns]

numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
    'metrics_conversions', 'metrics_ctr', 'metrics_average_cpc'
]
string_cols_to_fill = [
    'campaign_name', 'campaign_advertising_channel_type', 'ad_group_name',
    'detail_placement_view_placement', 'detail_placement_view_placement_type',
    'detail_placement_view_display_name', 'detail_placement_view_group_placement_target_url'
]

df_placement_performance = placement_performance_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct", col("metrics_ctr") * 100) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        "campaign_advertising_channel_type",
        "ad_group_id",
        "ad_group_name",
        col("detail_placement_view_placement").alias("placement"),
        col("detail_placement_view_placement_type").alias("placement_type"),
        col("detail_placement_view_display_name").alias("display_name"),
        col("detail_placement_view_group_placement_target_url").alias("group_placement_target_url"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"
    )

print("--- Placement Performance Schema ---")
df_placement_performance.printSchema()
display(df_placement_performance.limit(10))

# COMMAND ----------

"""
Displays the first 3 rows and schema of the group placement performance DataFrame for inspection.
"""
display(group_placement_performance_df.limit(3))
group_placement_performance_df.printSchema()

# COMMAND ----------

# DBTITLE 1,group_placement_performance
"""
Group placement performance (aggregated placement groups)
"""
new_column_names = [c.replace('.', '_') for c in group_placement_performance_df.columns]

numeric_cols_to_fill = [
    'metrics_impressions', 'metrics_clicks', 'metrics_cost_micros',
    'metrics_conversions', 'metrics_ctr', 'metrics_average_cpc'
]
string_cols_to_fill = [
    'campaign_name', 'campaign_advertising_channel_type', 'ad_group_name',
    'group_placement_view_placement', 'group_placement_view_placement_type',
    'group_placement_view_display_name'
]

df_group_placement_performance = group_placement_performance_df.toDF(*new_column_names) \
    .fillna(0, subset=numeric_cols_to_fill) \
    .fillna('Unknown', subset=string_cols_to_fill) \
    .withColumn("ctr_pct", col("metrics_ctr") * 100) \
    .withColumn("cost_inr", col("metrics_cost_micros") / 1000000) \
    .withColumn("cpc_inr", col("metrics_average_cpc") / 1000000) \
    .select(
        "segments_date",
        "campaign_id",
        "campaign_name",
        "campaign_advertising_channel_type",
        "ad_group_id",
        "ad_group_name",
        col("group_placement_view_placement").alias("placement"),
        col("group_placement_view_placement_type").alias("placement_type"),
        col("group_placement_view_display_name").alias("display_name"),
        "metrics_impressions",
        "metrics_clicks",
        "metrics_conversions",
        "ctr_pct",
        "cpc_inr",
        "cost_inr"
    )

print("--- Group Placement Performance Schema ---")
df_group_placement_performance.printSchema()
display(df_group_placement_performance.limit(10))

# COMMAND ----------

# DBTITLE 1,Saving as delta tables
"""
Ensures the silver database exists and saves all transformed DataFrames to the silver layer as Delta tables.
"""
# Ensure database exists
spark.sql("CREATE DATABASE IF NOT EXISTS googleads_silver")

dataframes_to_save = {
    "campaigns": df_core_campaign,
    "keywords": df_search_keyword,
    "search_terms": df_search_term,
    "conversions": df_conversion,
    "ad_copy": df_ad_copy,
    "devices": df_optimization_device,
    "genders": df_audience_gender,
    "ages": df_audience_age,
    "impressions": df_competitive_impression,
    "networks": df_network,
    "display_ads": df_display_ad,
    "calls": df_call_lead_generation,
    "locations": df_geographic_enriched,
    "geo_constants": df_geo_target_constant,
    "ads_data":df_ads_data,
    "ads_performance":df_ads_performance,
    "campaign_assets": df_campaign_asset,
    "ad_group_ad_assets": df_ad_group_ad_asset_view,
    "assets": df_asset,
    "conversion_actions": df_conversion_action,
    "placements": df_placement_performance,
    "group_placements": df_group_placement_performance  
}

# Loop through the dictionary and save each DataFrame in googleads_silver
for table_name, df in dataframes_to_save.items():
    try:
        full_table_name = f"googleads_silver.{table_name}"
        print(f"--- Saving {full_table_name}... ---")

        df.write.format("delta") \
            .option("overwriteSchema", "true")\
            .mode("overwrite") \
            .saveAsTable(full_table_name)

        print(f"SUCCESS: DataFrame successfully saved as Delta table '{full_table_name}'.")

    except Exception as e:
        print(f"ERROR saving {full_table_name}: {e}")

print("\n--- All DataFrames have been processed. ---")


# COMMAND ----------

# MAGIC %sql SHOW COLUMNS IN googleads_silver.ads_data

# COMMAND ----------

# DBTITLE 1,Row counts & Schemas
# 1. Configuration
catalog_name = "workspace" 
schema_name = "googleads_silver"
full_schema_path = f"{catalog_name}.{schema_name}"

print(f"{'='*60}")
print(f"Source: {full_schema_path}")
print(f"{'='*60}\n")

try:
    # Fetch list of tables from Unity Catalog
    tables = spark.catalog.listTables(full_schema_path)
    
    total_rows_all_tables = 0
    table_count = 0

    for table in tables:
        full_table_name = f"{full_schema_path}.{table.name}"
        
        try:
            # Load the table strictly from the Metastore/Storage
            df_audit = spark.table(full_table_name)
            
            # 1. Get Row Count
            # [VALIDATED] .count() forces a read of the Delta log/parquet files
            row_count = df_audit.count()
            total_rows_all_tables += row_count
            table_count += 1
            
            # 2. Print Report Block
            print(f"\n{'-'*20} TABLE: {table.name} {'-'*20}")
            print(f"LOCATION: {full_table_name}")
            print(f"ROW COUNT: {row_count:,}")
            
            print(f"{'-'*10} SCHEMA {'-'*10}")
            df_audit.printSchema()
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"[ERROR] Could not read table {table.name}: {e}\n")

    # Final Summary
    print(f"SUMMARY:")
    print(f"Total Tables Inspected: {table_count}")
    print(f"Total Rows in Silver Layer: {total_rows_all_tables:,}")

except Exception as e:
    print(f"[CRITICAL ERROR] Could not list tables in {full_schema_path}.")
    print(f"Error Details: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Google Ads Metric Definitions
# MAGIC
# MAGIC ## 1. Fundamental Metrics
# MAGIC
# MAGIC ### **Impressions**
# MAGIC * **Definition:** The number of times your advertisement was displayed on a screen. This is the base metric for visibility.
# MAGIC * **Context:** An impression is counted each time your ad is shown on a search result page or other site on the Google Network.
# MAGIC
# MAGIC ### **CPC (Cost Per Click)**
# MAGIC * **Definition:** The actual price you pay for each click in your pay-per-click (PPC) marketing campaigns.
# MAGIC * **Formula:** $$CPC = \frac{\text{Total Cost}}{\text{Number of Clicks}}$$
# MAGIC * **Example:** If you spend $100 and get 50 clicks, your CPC is $2.00.
# MAGIC
# MAGIC ### **CTR (Click-Through Rate)**
# MAGIC * **Definition:** The ratio of users who click on a specific link to the number of total users who view a page, email, or advertisement. It measures the relevance of your ad.
# MAGIC * **Formula:** $$CTR = \left( \frac{\text{Number of Clicks}}{\text{Number of Impressions}} \right) \times 100$$
# MAGIC * **Example:** If your ad had 1,000 impressions and one click, the CTR is 0.1%.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Conversion & Efficiency Metrics
# MAGIC
# MAGIC ### **CVR (Conversion Rate)**
# MAGIC * **Definition:** The average number of conversions per ad interaction, shown as a percentage. It measures how effective your landing page is at turning visitors into customers.
# MAGIC * **Formula:** $$CVR = \left( \frac{\text{Number of Conversions}}{\text{Number of Clicks}} \right) \times 100$$
# MAGIC * **Example:** If you had 50 clicks and 2 sales (conversions), your CVR is 4%.
# MAGIC
# MAGIC ### **CPA (Cost Per Acquisition / Action)**
# MAGIC * **Definition:** The average amount you have been charged for a conversion from your ad.
# MAGIC * **Formula:** $$CPA = \frac{\text{Total Cost}}{\text{Number of Conversions}}$$
# MAGIC * **Example:** If you spent $100 and got 2 sales, your CPA is $50.
# MAGIC
# MAGIC ### **ROAS (Return on Ad Spend)**
# MAGIC * **Definition:** A marketing metric that measures the amount of revenue your business earns for each dollar it spends on advertising.
# MAGIC * **Formula:** $$ROAS = \frac{\text{Revenue from Ads}}{\text{Cost of Ads}}$$
# MAGIC * **Example:** If you spent $100 on ads and made $500 in sales, your ROAS is 5 (or 500%).