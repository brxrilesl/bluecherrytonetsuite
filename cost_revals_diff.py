import pandas as pd
import numpy as np
from datetime import datetime
import sys

# Settings
# Diff returns less items
diff = True
multi_file_output = True

# Function to save DataFrame to CSV
def save_dataframe_to_csv(df, base_path, multi_file_output, file_prefix):
    if multi_file_output:
        if len(df) > 15000:
            # Split DataFrame into chunks of 15000 rows and save each chunk to a separate CSV
            for i, chunk in enumerate(np.array_split(df, range(15000, len(df), 15000))):
                chunk.to_csv(f"{base_path}_{file_prefix}_{i}.csv", index=False)
        else:
            # Save the entire DataFrame to a single CSV if rows are less than or equal to 15000
            df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)
    else:
        # Save the entire DataFrame to a single CSV regardless of the number of rows
        df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)

# Generate base path for output files using current date and time
base_path = "output/" + datetime.now().strftime('%Y-%m-%d_%H-%M-%S_')

# Define paths to input CSV files
netsuite_children_csv_path = "input_files/netsuite_children_items-20240618.csv"
cost_revals_csv_path = "input_files/" + "netsuite_cost_revals.csv"

# Read input CSV files into DataFrames
netsuite_children_df = pd.read_csv(netsuite_children_csv_path)
cost_revals_df = pd.read_csv(cost_revals_csv_path)

# Extract 'Item External ID' from 'External ID' in cost_revals_df
cost_revals_df['Item External ID'] = cost_revals_df['External ID'].str.split(' - ').str[0]

# Find rows in netsuite_children_df where 'External ID' is not in cost_revals_df's 'Item External ID'
missing_revals_df = netsuite_children_df[~netsuite_children_df['External ID'].isin(cost_revals_df['Item External ID'])]

# Save the resulting DataFrame to CSV
save_dataframe_to_csv(missing_revals_df, base_path, multi_file_output, "missing_cost_revals")
