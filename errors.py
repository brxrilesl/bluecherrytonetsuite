import pandas as pd

import os
# Read every CSV in the results directory
results_dir = 'results'
csv_files = [f for f in os.listdir(results_dir) if f.endswith('.csv')]
dfs = [pd.read_csv(os.path.join(results_dir, f)) for f in csv_files]

# Concatenate them into one DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

# Delete duplicates based on the 'error' column
combined_df.drop_duplicates(subset='Error', inplace=True)

# Keep only the 'error' column
combined_df = combined_df[['Error']]

# Re-export the combined DataFrame to a new CSV file
combined_df.to_csv(os.path.join(results_dir, 'combined_results.csv'), index=False)
