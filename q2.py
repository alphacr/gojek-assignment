import pandas as pd
from pathlib import Path
import json

customers_dataset_path = r'L:\Github\gojek-assignment\dataset\customers.csv'
deliveries_dataset_path = r'L:\Github\gojek-assignment\dataset\deliveries.csv'
orders_dataset_path = r'L:\Github\gojek-assignment\dataset\orders.csv'
products_dataset_path = r'L:\Github\gojek-assignment\dataset\products.csv'


def cleanPhoneNumber(phone_number):
    if phone_number[0] == "+":
        return "0" + phone_number[3:]
    elif phone_number.isnumeric():
        if phone_number[0] != "0":
            return "0" + phone_number
        else: return phone_number
    else:
        return "UNKNOWN"

def isActive(latest_order):
    today = pd.to_datetime("today").normalize()
    last_6_month = today - pd.DateOffset(months=6)

    latest_order_datetime = pd.to_datetime(latest_order)
    return latest_order_datetime > last_6_month


customers_df = pd.read_csv(customers_dataset_path)
deliveries_df = pd.read_csv(deliveries_dataset_path)
orders_df = pd.read_csv(orders_dataset_path)
products_df = pd.read_csv(products_dataset_path)

selected_customers_df = customers_df[['customer_code', 'name','phone_number', 'id']]
selected_customers_df['customer_id'] = customers_df['id']

# Clean customers table
selected_customers_df['customer_code'] = selected_customers_df['customer_code'].str.upper()
selected_customers_df['customer_name'] = selected_customers_df['name'].str.replace('[^a-zA-Z0-9]', ' ')


# phone_number
selected_customers_df['phone_number'] = selected_customers_df['phone_number'].apply(cleanPhoneNumber)
customer_orders_table = selected_customers_df.merge(orders_df, on='customer_id')

# is_active
customer_orders_table["latest_order"] = customer_orders_table.groupby(['customer_id'])['order_date'].transform(max)
customer_orders_table["is_active"] = customer_orders_table["latest_order"].apply(isActive)

# total_value
successful_customer_orders_table = customer_orders_table.loc[customer_orders_table.is_cancelled != True]
cust_ordered_products_table = successful_customer_orders_table.merge(products_df, left_on ="product_id", right_on="id")
cust_ordered_products_table["total_value"] = cust_ordered_products_table['price'].groupby(cust_ordered_products_table['customer_id']).transform('sum')

# loyalty_level
cust_ordered_products_table["loyalty_level"] = "BRONZE" # TODO FINISH ME
cust_ordered_products_table["name"] = cust_ordered_products_table["customer_name"]

selected_cust_table = cust_ordered_products_table[["customer_code", "name", "phone_number", "is_active", "loyalty_level", "total_value"]]
result = selected_cust_table.drop_duplicates()

result.to_csv('q2_result.csv', index=False)
