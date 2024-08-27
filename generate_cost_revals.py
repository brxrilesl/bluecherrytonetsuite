import pandas as pd
import numpy as np
from datetime import datetime
import sys


parent_cost_revals = pd.read_csv('input_files/parent_cost_revals_20240617.csv')
netsuite_items = pd.read_csv('input_files/netsuite_children.csv')
netsuite_locations = pd.read_csv('input_files/netsuite_locations.csv')
netsuite_subs = pd.read_csv('input_files/netsuite_subs.csv')

# Create a dictionary from parent_cost_revals for quick access
cost_revals_dict = parent_cost_revals.drop_duplicates(subset='ItemId').set_index('ItemId').to_dict('index')

# Initialize an empty list for the final upload data
final_upload_data = []

# Loop through each child in netsuite_items
for _, child_row in netsuite_items.iterrows():
    # Get the Parent value to match in cost_revals
    subitem_of = child_row['Parent']
    # Check if the Parent exists in cost_revals_dict
    if subitem_of in cost_revals_dict:
        # Loop through each location in netsuite_locations
        for _, location_row in netsuite_locations.iterrows():
            # Construct the final external ID
            final_external_id = f"{child_row['External ID']} - {location_row['Name']} - {cost_revals_dict[subitem_of]['Standard Cost']}"
            # Append the data to final_upload_data
            final_upload_data.append({
                'External ID': final_external_id,
                'Item External ID': child_row['External ID'],
                'Date': datetime.now().strftime('%m/%d/%Y'),
                'Location Internal ID': location_row['Internal ID'],
                'Subsidiary Internal ID': netsuite_subs.loc[netsuite_subs['Name'] == location_row['Subsidiary'], 'Internal ID'].values[0],
                'Standard Cost': cost_revals_dict[subitem_of]['Standard Cost'],
            })

# Convert final_upload_data to DataFrame
final_upload = pd.DataFrame(final_upload_data)
final_upload = final_upload.dropna(subset=['Standard Cost'])
# Prefix for the output file
prefix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_")

print("Final Upload DataFrame Statistics:")
print(f"Rows: {len(final_upload)}, Columns: {len(final_upload.columns)}")
print("Column Names:", final_upload.columns.tolist())

file_limit = 22000
if len(final_upload) > file_limit:
    for i, chunk in enumerate(np.array_split(final_upload, range(file_limit, len(final_upload), file_limit))):
        chunk.to_csv(f"output/{prefix}_final_costs_upload_{i}.csv", index=False)
else:
    final_upload.to_csv(f"output/{prefix}_final_costs_upload_single_file.csv", index=False)

    # Print statistics for input and output files
    input_files = ['input_files/parent_cost_revals_20240617.csv', 'input_files/netsuite_children.csv', 'input_files/netsuite_locations.csv', 'input_files/netsuite_subs.csv']
    output_files = [f"output/{prefix}_final_costs_upload_single_file.csv"] if len(final_upload) <= file_limit else [f"output/{prefix}_final_costs_upload_{i}.csv" for i, _ in enumerate(np.array_split(final_upload, range(file_limit, len(final_upload), file_limit)))]

    print("Input Files Statistics:")
    for file in input_files:
        df = pd.read_csv(file)
        print(f"{file}: Rows={len(df)}, Columns={len(df.columns)}")

    print("\nOutput Files Statistics:")
    for file in output_files:
        df = pd.read_csv(file)
        print(f"{file}: Rows={len(df)}, Columns={len(df.columns)}")
