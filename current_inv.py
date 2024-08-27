import pandas as pd
import glob
from datetime import datetime

def load_dataframes():
    # Load the CSV files into DataFrames
    # rmplog_df contains raw inventory data from RMPL logs
    rmplog_df = pd.read_csv('input_files/rmpraw.csv')
    # loaded_df contains previously loaded inventory data
    loaded_df = pd.read_csv('input_files/Loaded.csv')
    # missing_costs_df contains items with missing cost information
    missing_costs_df = pd.read_csv("input_files/2024-06-18_09-15-47__missing_cost_revals.csv")
    # netsuite_children_items_df contains NetSuite child items information
    netsuite_children_items_df = pd.read_csv('input_files/netsuite_children_items-20240618.csv')
    # rmplog_lot_dict contains mapping of RMPL log lots to locations or sub locations
    rmplog_lot_dict = pd.read_csv('input_files/rmplog_lot_mapping.csv').set_index('Lot').to_dict()
    return rmplog_df, loaded_df, missing_costs_df, netsuite_children_items_df, rmplog_lot_dict


def process_excel_files(file_path, base_output_path, netsuite_children_items_df, missing_costs_df):
    # Find all Excel files in the specified directory
    excel_files = glob.glob(file_path)
    print(f"Found {len(excel_files)} Excel files to process.")
    for file in excel_files:
        process_single_file(file, base_output_path, netsuite_children_items_df, missing_costs_df)

def process_single_file(file, base_output_path, netsuite_children_items_df, missing_costs_df):
    print(f"Processing file: {file}")
    # Extract location from the file name
    location = file.split('00 ')[-1].replace('.xlsx', '')
    # Read the Excel file into a DataFrame
    df = pd.read_excel(file, header=1)
    df['Location'] = location.upper()
    df['Memo'] = 'SB - Retail Inventory import - RL'
    df.rename(columns={'SKU': 'Item External ID', 'Available': 'QTY'}, inplace=True)
    # Generate a transaction external ID
    transaction_external_id = 'IA' + location.replace(' ', '').upper() + datetime.now().strftime('%Y%m%d')
    df['Transaction External ID'] = transaction_external_id
    missing_costs_df.rename(columns={'External ID': 'Item External ID'}, inplace=True)
    # Filter items based on netsuite_children_items_df
    df = df[df['Item External ID'].isin(netsuite_children_items_df['External ID'])]
    # Separate items with missing costs
    cost_filtered_items = df[df['Item External ID'].isin(missing_costs_df['Item External ID'])]
    df = df[~df['Item External ID'].isin(missing_costs_df['Item External ID'])]
    # Separate items with negative quantities
    negative_qty_items = df[df['QTY'] < 0]
    df = df[df['QTY'] >= 0]
    df['Inventory Status'] = 'Good'
    cost_filtered_items['Inventory Status'] = 'Good'
    negative_qty_items['Inventory Status'] = 'Good'
    df.drop(columns=['Product'], inplace=True)
    save_dataframes(df, cost_filtered_items, negative_qty_items, base_output_path, location)

def save_dataframes(df, cost_filtered_items, negative_qty_items, base_output_path, location):
    # Define output file paths
    output_file = base_output_path + f'inventory_{location}.csv'
    filter_output_file = base_output_path + f'inventor_missing_costs_{location}.csv'
    negative_qty_output_file = base_output_path + f'inventory_negative_qty_{location}.csv'
    # Save DataFrames to CSV files
    df.to_csv(output_file, index=False)
    cost_filtered_items.to_csv(filter_output_file, index=False)
    negative_qty_items.to_csv(negative_qty_output_file, index=False)
    print(f"Saved DataFrame to {output_file}")

def process_rmplog_dataframes(rmplog_df, base_output_path, netsuite_children_items_df, missing_costs_df, rmplog_lot_dict):
    # Rename columns for consistency
    rmplog_df.rename(columns={'Item': 'External ID', 'On-Hand Qty': 'QTY', 'Lot': 'Lot'}, inplace=True)
    # Map Lot to Location
    rmplog_df['Location'] = rmplog_df['Lot'].map(rmplog_lot_dict['Location or Sub Location'])
    # Separate out Lot != 'N' from the main df to a rmplog_sublocations_df
    rmplog_sublocations_df = rmplog_df[rmplog_df['Lot'] != 'N']
    rmplog_df = rmplog_df[rmplog_df['Lot'] == 'N']
    # Separate wholesale and e-commerce data
    rmplog_wholesale_df = rmplog_df[rmplog_df['Company'] == 'BRIX_B2B']
    rmplog_ecom_df = rmplog_df[rmplog_df['Company'] == 'BRIX']

    # Process the sublocations DataFrame
    process_rmplog_dataframe(rmplog_sublocations_df, base_output_path, 'rmplog_sublocations', netsuite_children_items_df, missing_costs_df, rmplog_lot_dict)
    process_rmplog_dataframe(rmplog_wholesale_df, base_output_path, 'rmplog_wholesale', netsuite_children_items_df, missing_costs_df, rmplog_lot_dict)
    process_rmplog_dataframe(rmplog_ecom_df, base_output_path, 'rmplog_ecom', netsuite_children_items_df, missing_costs_df, rmplog_lot_dict)

def process_rmplog_dataframe(rmplog_df, base_output_path, suffix, netsuite_children_items_df, missing_costs_df, rmplog_lot_dict):
    rmplog_df['Memo'] = 'SB - RMPLOG Inventory import - RL'
    # Generate a transaction external ID
    transaction_external_id = 'IARMPL' + suffix.upper() + datetime.now().strftime('%Y%m%d')
    rmplog_df['Transaction External ID'] = transaction_external_id
    # Filter items based on netsuite_children_items_df
    rmplog_df = rmplog_df[rmplog_df['External ID'].isin(netsuite_children_items_df['External ID'])]
    # Separate items with missing costs
    cost_filtered_items = rmplog_df[rmplog_df['External ID'].isin(missing_costs_df['Item External ID'])]
    rmplog_df = rmplog_df[~rmplog_df['External ID'].isin(missing_costs_df['Item External ID'])]
    # Separate items with negative quantities
    negative_qty_items = rmplog_df[rmplog_df['QTY'] < 0]
    rmplog_df = rmplog_df[rmplog_df['QTY'] > 0]
    rmplog_df['Inventory Status'] = 'Good'
    cost_filtered_items['Inventory Status'] = 'Good'
    negative_qty_items['Inventory Status'] = 'Good'

    save_rmplog_dataframes(rmplog_df, cost_filtered_items, negative_qty_items, base_output_path, suffix)

def save_rmplog_dataframes(rmplog_df, cost_filtered_items, negative_qty_items, base_output_path, suffix):
    # Define output file paths
    output_file = base_output_path + f'inventory_{suffix}.csv'
    filter_output_file = base_output_path + f'inventory_missing_costs_{suffix}.csv'
    negative_qty_output_file = base_output_path + f'inventory_negative_qty_{suffix}.csv'
    # Save DataFrames to CSV files
    rmplog_df.to_csv(output_file, index=False)
    cost_filtered_items.to_csv(filter_output_file, index=False)
    negative_qty_items.to_csv(negative_qty_output_file, index=False)
    print(f"Saved RMPL {suffix.capitalize()} DataFrame to {output_file}")

def main():
    # Define base output path with current timestamp
    base_output_path = "output/" + datetime.now().strftime('%Y-%m-%d_%H-%M-%S_')
    file_path = 'excelsheets/inventory_quantities/*.xlsx'
    # Load DataFrames from CSV files
    rmplog_df, loaded_df, missing_costs_df, netsuite_children_items_df, rmplog_lot_dict = load_dataframes()
    # Process Excel files
    process_excel_files(file_path, base_output_path, netsuite_children_items_df, missing_costs_df)
    # Process RMPL log DataFrames
    process_rmplog_dataframes(rmplog_df, base_output_path, netsuite_children_items_df, missing_costs_df, rmplog_lot_dict)

if __name__ == "__main__":
    main()
