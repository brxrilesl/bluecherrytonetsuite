import pandas as pd
from datetime import datetime

# Load the NS_ALL_children.csv file
ns_all_children_df = pd.read_csv('input_files/NS_ALL_children.csv', encoding='latin1')

# Load the PO_SKUS.csv file
po_skus_df = pd.read_csv('input_files/PO_SKUS.csv', encoding='latin1')

# Print the columns of the two dataframes
print("Columns in ns_all_children_df:", ns_all_children_df.columns)
print("Columns in po_skus_df:", po_skus_df.columns)

# Find all the entries in the SKU column of po_skus_df that do not appear in the External ID column of ns_all_children_df
unique_skus = po_skus_df[~po_skus_df['SKU'].isin(ns_all_children_df['External ID'])]

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the output file name with current date and time prefix
output_file_name = f'output/{current_time}_unique_skus.csv'

# Save the unique SKUs dataframe to the output folder
unique_skus.to_csv(output_file_name, index=False)

print(unique_skus)
