# import pandas as pd #pandas is open source library for data analysis and manipulation in Python. It provides data structures and functions needed to manipulate structured data seamlessly.

# df = pd.read_csv("data/sales.csv") #df is pandas DataFrame object that reads the CSV file located at "data/sales.csv" and stores the data in a tabular format. The read_csv function is used to read the CSV file and convert it into a DataFrame.
# # print(df)

# # inspect the data / extraction

# # print(df.head()) # print the first 5 rows of the DataFrame

# # print(df.tail()) # print the last 5 rows of the DataFrame 

# # print(df.shape)   # print the shape of the DataFrame, rows and columns

# # print(df.columns)     # print the column names of the DataFrame

# # print(df.dtypes)    # print the data types of each column in the DataFrame

# # print(df.isnull().sum())    # print the number of missing values in each column

# #transformation of data

# df['total'] = df['quantity'] * df['price'] # create a new column 'Total' by multiplying 'Quantity' and 'Price'
# # print("\n total amount for each order: ")
# # print (df['total']) 

# total_revenue = df['total'].sum() # calculate the total revenue by summing the 'Total' column
# # print("\nTotal Revenue: ", total_revenue) # print the total revenue

# average_order_value = df['total'].mean() # calculate the average order value by taking the mean of the 'Total' column
# # print("\nAverage Order Value: ", average_order_value) # print the average order value

# print("\n")

# #using loc[] to find the row with the highest order value
# highest_order = df.loc[df["total"].idxmax()]    #find the row with the highest order value by using idxmax() to get the index of the maximum value in the 'Total' column and then using loc[] to retrieve the corresponding row
# # print("Highest Order:")
# # print(highest_order)

# #revenue by city
# revenue_by_city = (
#     df.groupby("city")["total"]     #group the DataFrame by 'City' and select the 'Total' column
#     .sum()          #group the DataFrame by 'City' and sum the 'Total' column for each city
#     .reset_index()  #reset the index of the resulting DataFrame after grouping by 'City' and summing the 'Total' column
# )
# # print(revenue_by_city)

# #revenue by product
# revenue_by_product = (
#     df.groupby("product")["total"]
#     .sum()
#     .reset_index()
# )
# # print(revenue_by_product)

# #sort
# revenue_by_product = revenue_by_product.sort_values(
#     "total",
#     ascending=False
# )
# # print(revenue_by_product)

# #revenue by category
# revenue_by_category = (
#     df.groupby("category")["total"]
#     .sum()
#     .reset_index()
# )
# # print(revenue_by_category)

# #remove duplicates
# df = df.drop_duplicates() # remove duplicate rows from the DataFrame using drop_duplicates() method
# # print(len(df))
# # print("\n")

# # print(df.isnull().sum())

# #find invalid records 
# invalid_records = df.loc[df.isnull().any(axis=1)]

# #copy invalid records to a new CSV file and print the contents of the new CSV file
# invalid_records.to_csv(
#     "output/invalid_records.csv",
#     index=False
# )
# inv = pd.read_csv("output/invalid_records.csv")
# # print(inv)

# #remove invalid records
# df = df.dropna() # remove rows with missing values from the DataFrame using dropna() method
# # print(df)

# #save the cleaned data to a new CSV file
# df.to_csv(
#     "output/clean_sales.csv",
#     index=False
# )

# #save the revenue by city, product, and category to new CSV files
# revenue_by_city.to_csv(
#     "output/revenue_by_city.csv",
#     index=False
# )
# revenue_by_product.to_csv(
#     "output/revenue_by_product.csv",
#     index=False
# )
# revenue_by_category.to_csv(
#     "output/revenue_by_category.csv",
#     index=False
# )

#----------final script----------

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

    df["total_amount"] = (
        df["quantity"] * df["price"]
    )

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