import pandas as pd
import logging

# -----------------------------
# SETUP LOGGING
# -----------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def run_etl():

    logging.info("ETL pipeline started")


    # -----------------------------
    # EXTRACT
    # -----------------------------

    df = pd.read_csv("data/sales.csv")

    logging.info(f"Loaded {len(df)} records")


    # -----------------------------
    # INSPECT
    # -----------------------------

    # print("\nDataset information:")
    # print(df.info())

    logging.info("Checking for missing values")
    print(df.isnull().sum()) # print the number of missing values in each column


    # -----------------------------
    # CLEAN
    # -----------------------------

    # duplicate orders
    duplicate_records = df[ df.duplicated( subset=["order_id"], keep="first")]

    # Save missing records
    missing_records = df.loc[df.isnull().any(axis=1)]
    
    invalid_records = pd.concat( [duplicate_records, missing_records] ).drop_duplicates()

    logging.warning (f"Found {len(invalid_records)} invalid records")

    if len(invalid_records) > 0:
        invalid_records.to_csv(
            "output/invalid_records.csv",
            index=False
        )

    # Remove records with missing price
    df = df.dropna()

    logging.info(f"Clean records remaining: {len(df)}")


    # -----------------------------
    # TRANSFORM
    # -----------------------------

    df["total_amount"] = (df["quantity"] * df["price"])

    df["final_amount"] = df["total_amount"] - df["discount"]

    logging.info ("Calculated total_amount and final_amount")

    # -----------------------------
    # ANALYZE
    # -----------------------------

    total_revenue = df["total_amount"].sum()

    average_order = df["total_amount"].mean()

    # print(f"\nTotal Revenue:{total_revenue}")

    # print("Average Order Value:", average_order)

    logging.info(f"Total Revenue: {total_revenue}")
    logging.info(f"Average Order Value: {average_order}")

    # Revenue by city

    revenue_by_city = (
        df.groupby("city")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )


    # Revenue by product

    revenue_by_product = (
        df.groupby("product")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )


    # Revenue by category

    revenue_by_category = (
        df.groupby("category")["total_amount"]
        .sum()
        .reset_index()
        .sort_values("total_amount", ascending=False)
    )


    # -----------------------------
    # LOAD
    # -----------------------------

    df.to_csv(
        "output/clean_sales.csv",
        index=False
    )

    revenue_by_city.to_csv(
        "output/revenue_by_city.csv",
        index=False
    )

    revenue_by_product.to_csv(
        "output/revenue_by_product.csv",
        index=False
    )

    revenue_by_category.to_csv(
        "output/revenue_by_category.csv",
        index=False
    )


    logging.info("ETL pipeline completed successfully")

    return df 

if __name__ == "__main__":
    setup_logging()
    df = run_etl()