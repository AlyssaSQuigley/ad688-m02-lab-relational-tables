from pyspark.sql import SparkSession, functions as F
import glob
import os

spark = SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()

files = glob.glob("data/MET_CareerCompass_2026/*.parquet")
df = spark.read.parquet(*files)

# --- Company table (build first, so we can assign COMPANY_ID back to jobs) ---
company_df = df.select("COMPANY_NAME", "COMPANY_RAW", "COMPANY_IS_STAFFING").distinct() \
    .withColumn("COMPANY_ID", F.monotonically_increasing_id())

# Join COMPANY_ID back onto the main df so Job_Postings can reference it
df_with_company = df.join(
    company_df.select("COMPANY_NAME", "COMPANY_RAW", "COMPANY_ID"),
    on=["COMPANY_NAME", "COMPANY_RAW"],
    how="left"
)

# --- Table 1: Job_Postings ---
job_postings_df = df_with_company.select(
    "ID", "TITLE_RAW", "TITLE_CLEAN", "POSTED", "EXPIRED",
    "SALARY_FROM", "SALARY_TO", "MIN_YEARS_EXPERIENCE", "MAX_YEARS_EXPERIENCE",
    "SKILLS", "SPECIALIZED_SKILLS", "SOFTWARE_SKILLS", "EMPLOYMENT_TYPE", "COMPANY_ID"
).distinct()

# --- Table 2: Company ---
company_final_df = company_df.select(
    "COMPANY_ID", "COMPANY_NAME", "COMPANY_RAW", "COMPANY_IS_STAFFING"
)

# --- Table 3: Job_Location ---
job_location_df = df.select("ID", "CITY", "STATE", "COUNTY", "LOCATION").distinct()

# --- Table 4: SOC_Details ---
soc_details_df = df.select(
    "ID", "SOC_2", "SOC_2_NAME", "SOC_3", "SOC_3_NAME",
    "SOC_4", "SOC_4_NAME", "SOC_5", "SOC_5_NAME"
).distinct()

# --- Table 5: LOT_Details ---
# NOTE: LOT_CAREER_AREA / LOT_OCCUPATION / LOT_SPECIALIZED_OCCUPATION do not exist
# in this dataset version. ONET / ONET_NAME are the closest available fields.
lot_details_df = df.select("ID", "ONET", "ONET_NAME").distinct() \
    .withColumn("LOT_CAREER_AREA", F.lit(None)) \
    .withColumn("LOT_CAREER_AREA_NAME", F.lit(None)) \
    .withColumn("LOT_OCCUPATION", F.lit(None)) \
    .withColumn("LOT_OCCUPATION_NAME", F.lit(None)) \
    .withColumnRenamed("ONET", "LOT_SPECIALIZED_OCCUPATION") \
    .select("ID", "LOT_CAREER_AREA", "LOT_CAREER_AREA_NAME", "LOT_OCCUPATION",
            "LOT_OCCUPATION_NAME", "LOT_SPECIALIZED_OCCUPATION", "ONET_NAME")

# --- Table 6: NAICS_Details ---
naics_details_df = df.select(
    "ID", "NAICS2", "NAICS2_NAME", "NAICS3", "NAICS3_NAME",
    "NAICS4", "NAICS4_NAME", "NAICS5", "NAICS5_NAME", "NAICS6", "NAICS6_NAME"
).distinct()

# --- Save all tables as CSV ---
os.makedirs("_output", exist_ok=True)

tables = {
    "job_postings": job_postings_df,
    "company": company_final_df,
    "job_location": job_location_df,
    "soc_details": soc_details_df,
    "lot_details": lot_details_df,
    "naics_details": naics_details_df,
}

for name, sdf in tables.items():
    pdf = sdf.toPandas()
    pdf.to_csv(f"_output/{name}.csv", index=False)
    print(f"Saved _output/{name}.csv with {len(pdf)} rows")

print("Done.")
