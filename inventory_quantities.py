import pandas as pd
import numpy as np
from datetime import datetime
import sys

ramplog_inventory_df = pd.read_csv('ramplog_quantities.csv')
netsuite_locations_df = pd.read_csv('netsuite_locations.csv')

ramplog_columns = [
    'Div', 'Loc Org', 'BRAND', 'Style', 'Color #', 'Style Name',
    'Color Name', 'Label', 'Dm/Pk', 'Size', 'SzBk', 'UOM', 'UPC Number',
    'Season', 'Location', 'Total QOH', 'Total Pick', 'Avail QOH',
    'Total Open', 'Total Ship To WIP', 'Total Received', 'Total Invoice',
    'Total Prod', 'Loc Std Cost', 'A Price Intr1', 'Loc Curr', 'P1',
    'P1 Name', 'P2', 'P2 Name', 'P3', 'P3 Name', 'Style Type'
]

output_df = pd.DataFrame(columns=[
    'Date', 'Posting Period', 'Reference #', 'Adjustment Account', 'Subsidiary',
    'Department', 'Class', 'Location', 'Memo', 'Transaction Order', 'Item',
    'New Qty', 'New Value'
])


def lookup_location_id(row_label, row_location):
    name = ''
    if row_label:
        name = row_label
    else:
        name = row_location
    netsuite_location_id = netsuite_locations_df.loc[netsuite_locations_df['Name'] == name, 'Internal ID'].values
    if netsuite_location_id.size > 0:
        return netsuite_location_id[0]
    else:
        return None

# Define a function to map values from ramplog_inventory_df to output_df
def map_ramplog_to_output(ramplog_df, output_df):
    for index, row in ramplog_df.iterrows():
        output_df = output_df._append({
            'Date': datetime.now().strftime('%Y-%m-%d'),
            'Posting Period': datetime.now().strftime('%Y-%m'),
            'Reference #': 'TODO',
            'Adjustment Account': 'Inventory Adjustment',
            'Subsidiary': row['Div'],
            'Department': row['Loc Org'],
            'Class': row['BRAND'],
            'Location': lookup_location_id(row['Location'], row['Label']),
            'Transaction Order': 'INVENWORK' + datetime.now().strftime('%Y%m%d%H%M%S')
            'Item': str(row['Style']).zfill(5) + "-" + row["Color #"],
            'New Qty': row['Total QOH'],
            'New Value': row['Loc Std Cost']
        }, ignore_index=True)
    return output_df

# Map the values
output_df = map_ramplog_to_output(ramplog_inventory_df, output_df)
print(output_df)

# Save the output to a new CSV file
output_df.to_csv('output_inventory_adjustments.csv', index=False)
