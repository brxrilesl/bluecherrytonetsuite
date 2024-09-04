import pandas as pd
import numpy as np
from datetime import datetime
import sys

# Settings
# Diff returns less items
check_if_exists_in_netsuite = False

multi_file_output = True
row_limit = 24000

# Filter Settings
use_filtered_item_list = False
filtered_item_list = "20240816_112829_unique_skus.csv"

style_master_csv_path = "input_files/StyleMaster_20240820.csv"
loaded_csv_path = "input_files/netsuite_children_20240830.csv"
drop_list_path = "input_files/filtered_drop_list.csv"
bluecherry_fob_path = "input_files/bluecherry_fob.csv"
pricing_data_csv_path = "input_files/pricing_extract.csv"
frozen_costs_data_path = "input_files/frozen_costs_july.csv"



use_season_filter = False
season_filster = "SP25"

print("Settings")
print(f"Diff: {check_if_exists_in_netsuite}")
print(f"Multi File Output: {multi_file_output}")

# Function to combine columns with a dash, without spaces
def combine_columns_with_dash(row, columns):
    return '-'.join([str(row[col]) for col in columns if col in row and pd.notnull(row[col])])

def save_dataframe_to_csv(df, base_path, multi_file_output, file_prefix):
    if multi_file_output:
        if len(df) > row_limit:
            for i, chunk in enumerate(np.array_split(df, range(row_limit, len(df), row_limit))):
                chunk.to_csv(f"{base_path}_{file_prefix}_{i}.csv", index=False)
        else:
            df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)
    else:
        df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)

def load_and_clean_csv(file_path, drop_na=True, dtype=None):
    df = pd.read_csv(file_path, dtype=dtype)
    if drop_na:
        df.dropna(how='all', inplace=True)
    print(f"{file_path}, Number of records: {len(df)}")
    return df

def process_style_master(style_master_df, vendor_mapping):
    style_master_df['Style'] = style_master_df['Style'].str.zfill(5)
    style_master_df['Style-Color'] = style_master_df['Style'] + '-' + style_master_df['Color #']
    style_master_df['VENDOR'] = style_master_df['VENDOR'].apply(lambda x: vendor_mapping[x] if x in vendor_mapping and vendor_mapping[x] != "" else x)
    remapped_vendors = style_master_df['VENDOR'].apply(lambda x: 1 if x in vendor_mapping.values() and x != "" else 0).sum()
    print(f"Number of vendors remapped: {remapped_vendors}")
    style_master_df['Style-Color-Size'] = style_master_df.apply(lambda row: combine_columns_with_dash(row, ['Style', 'Color #', 'SIZES']), axis=1)
    style_master_df.dropna(how='all', inplace=True)
    return style_master_df

def map_category(style_master_df, category_mapping):
    for index, row in style_master_df.iterrows():
        for col in ['P1', 'P2', 'P3']:
            style_master_df.at[index, col] = category_mapping[style_master_df.at[index, col]]
    return style_master_df

def create_child_production_items(style_master_df, size_code_mapping):
    child_production_items_df = pd.DataFrame()
    frozen_costs_data_df = load_and_clean_csv(frozen_costs_data_path)
    frozen_costs_data_df['Style-Color'] = frozen_costs_data_df.apply(lambda row: f"{row['Style']}-{row['Color #']}", axis=1)

    for size_column in size_code_mapping.values():
        child_production_items_df[size_column] = ''
    for index, row in style_master_df.iterrows():
        size_column = size_code_mapping.get(row['Size Code'], None)
        if size_column:
            child_production_items_df.at[index, size_column] = row['SIZES']
    child_production_items_df['UPC Number'] = style_master_df['UPC Number']
    child_production_items_df['Style'] = style_master_df['Style']
    child_production_items_df['Style-Color'] = style_master_df.apply(lambda row: combine_columns_with_dash(row, ['Style', 'Color #']), axis=1)
    child_production_items_df['Item Defined Cost'] = child_production_items_df['Style-Color'].map(frozen_costs_data_df.set_index('Style-Color')['Frozen Category Cost'])
    child_production_items_df['Style-Color-Size'] = style_master_df.apply(lambda row: combine_columns_with_dash(row, ['Style', 'Color #', 'SIZES']), axis=1)
    child_production_items_df['External ID'] = child_production_items_df['Style-Color-Size']
    child_production_items_df['Display Name'] = style_master_df['Style Name']
    child_production_items_df['Size Code'] = style_master_df['Size Code']
    child_production_items_df['Season'] = style_master_df['Season']
    child_production_items_df['Product Color'] = style_master_df['Color Name']
    child_production_items_df['HS Number'] = style_master_df['HS Number']
    child_production_items_df['Country of Origin'] = style_master_df['Country']
    child_production_items_df['Product Catagory 1'] = style_master_df['P1']
    child_production_items_df['Product Catagory 2'] = style_master_df['P2']
    child_production_items_df['Product Catagory 3'] = style_master_df['P3']
    child_production_items_df['GENDER'] = style_master_df['GENDER']
    child_production_items_df['Fabric Content'] = style_master_df['Style Detail Content Desc']
    child_production_items_df['Product | SMU'] = style_master_df['LINE TYPE/RM'].apply(lambda x: 'yes' if 'SMU' in x else 'No')
    child_production_items_df['Item Type'] = style_master_df['LINE TYPE/RM'].map(item_type_mapping)
    child_production_items_df["Prefered Vendor Code"] = style_master_df["VENDOR"]
    child_production_items_df["SubItem of"] = child_production_items_df["Style-Color"]
    child_production_items_df["Matrix Type"] = "Child Matrix Item"
    child_production_items_df["Matrix Option Product Size"] = child_production_items_df["Product Size"]
    child_production_items_df["Matrix Option Product O/S"] = child_production_items_df["Product O/S"]
    child_production_items_df["Matrix Option Product Bottoms Size"] = child_production_items_df["Product Bottoms Size"]
    child_production_items_df["Matrix Option Product Combo Sizes"] = child_production_items_df["Product Combo Sizes"]
    return child_production_items_df

def filter_diff_items(df, loaded_netsuite_children_items, column_name):
    loaded_list = loaded_netsuite_children_items[column_name].tolist()
    removed_items = df[df[column_name].isin(loaded_list)]
    df = df[~df[column_name].isin(loaded_list)]
    return df, removed_items

def create_sample_items(df):
    samples_df = df[df['Label'] == 'SAMPLE'].copy()
    df = df[df['Label'] != 'SAMPLE']
    df.drop('Label', axis=1, inplace=True)
    samples_df['External ID'] = 'S' + samples_df['External ID'].astype(str)
    samples_df['Style-Color'] = 'S' + samples_df['Style-Color'].astype(str)
    samples_df['Style-Color-Size'] = 'S' + samples_df['Style-Color-Size'].astype(str)
    samples_df['Is Sample'] = 'Yes'
    samples_df['Display Name'] = 'SAMPLE - ' + samples_df['Display Name'].astype(str)
    samples_df.drop('Label', axis=1, inplace=True)
    return df, samples_df

def create_parent_items(df, size_code_mapping):
    parent_items_df = df.drop_duplicates(subset=['Style-Color']).copy()
    parent_items_df.loc[:, 'Style-Color-Size'] = parent_items_df['Style-Color']
    parent_items_df.loc[:, 'External ID'] = parent_items_df['Style-Color']
    parent_items_df['Product Size'] = ''
    parent_items_df['Product Combo Sizes'] = ''
    parent_items_df['Product O/S'] = ''
    parent_items_df['Product Bottoms Size'] = ''
    parent_items_df['UPC Number'] = ''
    parent_items_df['Matrix Type'] = "Parent Matrix Item"
    parent_items_df["Matrix Option Product Size"] = ''
    parent_items_df["SubItem of"] = ''
    parent_items_df["Matrix Option Product O/S"] = ''
    parent_items_df["Matrix Option Product Bottoms Size"] = ''
    parent_items_df["Matrix Option Product Combo Sizes"] = ''
    for index, row in parent_items_df.iterrows():
        size_column = size_code_mapping.get(row['Size Code'], None)
        if size_column == 'Product Size':
            parent_items_df.at[index, "Matrix Item Name Template"] = "{itemid}-{custitem_psgss_product_size}"
        elif size_column == 'Product O/S':
            parent_items_df.at[index, "Matrix Item Name Template"] = "{itemid}-{custitem_bx_product_os}"
        elif size_column == 'Product Combo Sizes':
            parent_items_df.at[index, "Matrix Item Name Template"] = "{itemid}-{custitem_bx_product_combo_sz}"
        elif size_column == 'Product Bottoms Size':
            parent_items_df.at[index, "Matrix Item Name Template"] = "{itemid}-{custitem_bx_product_bottoms}"
    return parent_items_df

def create_sample_parent_items(samples_df):
    sample_parent_items_df = samples_df.drop_duplicates(subset=['Style-Color']).copy()
    sample_parent_items_df.loc[:, 'Style-Color-Size'] = sample_parent_items_df['Style-Color']
    sample_parent_items_df.loc[:, 'External ID'] = sample_parent_items_df['Style-Color']
    sample_parent_items_df['Product Size'] = ''
    sample_parent_items_df['Product Combo Sizes'] = ''
    sample_parent_items_df['Product O/S'] = ''
    sample_parent_items_df['Product Bottoms Size'] = ''
    sample_parent_items_df['UPC Number'] = ''
    sample_parent_items_df['Matrix Type'] = "Parent Matrix Item"
    sample_parent_items_df["Matrix Option Product Size"] = ''
    sample_parent_items_df["SubItem of"] = ''
    sample_parent_items_df["Matrix Option Product O/S"] = ''
    sample_parent_items_df["Matrix Option Product Bottoms Size"] = ''
    sample_parent_items_df["Matrix Option Product Combo Sizes"] = ''
    sample_parent_items_df['Is Sample'] = 'Yes'
    sample_parent_items_df['Display Name'] = 'SAMPLE - ' + sample_parent_items_df['Display Name']
    sample_parent_items_df.fillna('', inplace=True)
    return sample_parent_items_df

def create_flattened_vendors_df(bluecherry_fob_df):
    grouped = bluecherry_fob_df.groupby('Style-Color')
    flattened_vendors_df = pd.DataFrame()
    for name, group in grouped:
        row = {'Style-Color': name}
        for idx, (vendor, cost) in enumerate(zip(group['Vendor'], group['FOB Cost'])):
            row[f'Vendor_{idx+1}'] = vendor
            row[f'FOB Cost_{idx+1}'] = cost
        flattened_vendors_df = flattened_vendors_df._append(row, ignore_index=True)
    return flattened_vendors_df

base_path = "output/" + datetime.now().strftime('%Y-%m-%d_%H-%M-%S_')

# Size Code mapping based on the provided table
size_code_mapping = {
    'A': 'Product Size',
    'H': 'Product Size',
    'O': 'Product O/S',
    'B2': 'Product Bottoms Size',
    'B': 'Product Bottoms Size',
    'N': 'Product Combo Sizes',
    'M': 'Product Combo Sizes'
}

vendor_mapping = {
    "BU0125": "",
    "BU0192": "",
    "BU0795": "",
    "BU1046": "BU1243",
    "BU1077": "BU1275",
    "BU1213": "",
    "BU1240": "BU0140",
    "BU1264": "BU1275",
    "BU1270": "BU0042",
    "BU1281": "BU1384",
    "BU1380": "BU0140",
    "BU1381": "BU0140",
    "BU1385": "BU0080",
    "BU1386": "BU0080",
    "BU1387": "BU0080",
    "BU1458": "BU0042",
    "BU1552": "BU0110",
    "BU1648": "BU0080",
    "BU1664": "BU1472",
    "BU1670": "BU1480",
    "BU1671": "BU1481",
    "BU1672": "BU1482",
    "BU1673": "BU1483",
    "BU1674": "BU1484",
    "BU1675": "BU1485",
    "BU1676": "BU1486",
    "BU1677": "BU1487",
    "BU1678": "BU1488",
    "BU1679": "BU1489",
    "BU1680": "BU1490",
    "BU1681": "BU1491",
    "BU1682": "BU1492",
    "BU1683": "BU1493",
    "BU1684": "BU1494",
    "BU1685": "BU1495",
    "BU1686": "BU1496",
    "BU1687": "BU1497",
    "BU1688": "BU1498",
    "BU1689": "BU1499",
    "BU1690": "BU1500"
}

item_type_mapping = {
    "INLINE": "INLINE",
    "SMU-PACSN": "SMU",
    "SUPPLEMNT": "SUPPLEMENT",
    "SMU-INTL": "SMU",
    "SMU": "SMU",
    "SMU-BUCKL": "SMU",
    "SMU-AMZON": "SMU",
    "SMU-OFFPR": "SMU",
    "SMU-TILLY": "SMU",
    "ONLINE": "INLINE",
    "SMU-ZUMZ": "SMU",
    "SMU-NORDS": "SMU",
    "SMU-URBAN": "SMU",
    "WOMEN": "INLINE",
    "POP": "POP",
    "RMSKU": "RMSKU"
}

print("INPUT FILES")
print("----------------")

style_master_df = load_and_clean_csv(style_master_csv_path, dtype={'UPC Number': str, 'Style': str, 'Size Code': str, 'Color #': str})
style_master_df = process_style_master(style_master_df, vendor_mapping)
if use_filtered_item_list:
    generation_list_path = "input_files/" + filtered_item_list
    generation_list_df = load_and_clean_csv(generation_list_path, dtype={'SKU': str})
    style_master_df = style_master_df[style_master_df['Style-Color-Size'].isin(generation_list_df['SKU'])]
    print(f"Size of Style master after unique SKU filter: {len(style_master_df)}")

if use_season_filter:
    style_master_df = style_master_df[style_master_df['Season'] == season_filster]
    print(f"Size of Style master after season filter: {len(style_master_df)}")


loaded_netsuite_children_items = load_and_clean_csv(loaded_csv_path)
drop_list_df = load_and_clean_csv(drop_list_path)
bluecherry_fob_df = load_and_clean_csv(bluecherry_fob_path)
drop_list = drop_list_df['Style-Color'].tolist()
removed_items = style_master_df[style_master_df['Style-Color'].isin(drop_list)]
style_master_df = style_master_df[~style_master_df['Style-Color'].isin(drop_list)]

removed_items_csv_path = "output/" + datetime.now().strftime('%Y-%m-%d_%H-%M-%S_') + "removed_items.csv"
if not removed_items.empty:
    removed_items.to_csv(removed_items_csv_path, index=False)

category_mapping = pd.read_csv("input_files/" + "Category_Mapping.csv").set_index('Key')['Value'].to_dict()
print(f"Category mapping CSV path: 'Category_Mapping.csv', Number of records: {len(category_mapping)}")
style_master_df = map_category(style_master_df, category_mapping)

pricing_data_df = load_and_clean_csv(pricing_data_csv_path, dtype={'Style-Color': str, 'RET_PRICE': float, 'A_PRICE': float})
style_master_df['Style-Color'] = style_master_df['Style-Color'].str.replace(' ', '')
print(f"Pricing data CSV path: {pricing_data_csv_path}, Number of records: {len(pricing_data_df)}")
print("Processing......")

child_production_items_df = create_child_production_items(style_master_df, size_code_mapping)
child_production_items_df["Label"] = style_master_df["Label"]

# If the 'check_if_exists_in_netsuite' setting is enabled, filter out items from 'child_production_items_df' that are already present in 'loaded_netsuite_children_items' based on the 'External ID' column, and store the removed items in 'loaded_children_removed_items'
if check_if_exists_in_netsuite:
    child_production_items_df, loaded_children_removed_items = filter_diff_items(child_production_items_df, loaded_netsuite_children_items, 'External ID')

flattened_vendors_df = create_flattened_vendors_df(bluecherry_fob_df)
child_copy_df = child_production_items_df[['Style-Color', 'Style-Color-Size']].copy()
child_copy_with_vendors_df = child_copy_df.merge(flattened_vendors_df, on='Style-Color', how='left')

save_dataframe_to_csv(child_copy_with_vendors_df, base_path, multi_file_output, "vendor_purchase_prices")
items_with_blank_vendor_df = child_copy_with_vendors_df[child_copy_with_vendors_df['Vendor_1'].isnull()]
child_copy_with_vendors_df = child_copy_with_vendors_df[child_copy_with_vendors_df['Vendor_1'].notnull()]

save_dataframe_to_csv(items_with_blank_vendor_df, base_path, multi_file_output, "items_with_blank_vendor")
child_production_items_df, samples_df = create_sample_items(child_production_items_df)
sample_parent_items_df = create_sample_parent_items(samples_df)
parent_items_df = create_parent_items(child_production_items_df, size_code_mapping)

if check_if_exists_in_netsuite:
    parent_items_df, removed_loaded_parents = filter_diff_items(parent_items_df, loaded_netsuite_children_items, 'External ID')

# Separate SMU items to their own file
smu_items_df = child_production_items_df[child_production_items_df['Product | SMU'] == 'yes']
non_smu_items_df = child_production_items_df[child_production_items_df['Product | SMU'] != 'yes']
smu_parent_items_df = parent_items_df[parent_items_df['Style-Color'].isin(smu_items_df['Style-Color'])]
non_smu_parent_items_df = parent_items_df[~parent_items_df['Style-Color'].isin(smu_items_df['Style-Color'])]

child_production_pricing_df = child_production_items_df.join(pricing_data_df.set_index('Style-Color')[['RET_PRICE', 'A_PRICE']], on='Style-Color', how='left')
sample_pricing_df = samples_df.join(pricing_data_df.set_index('Style-Color')[['RET_PRICE', 'A_PRICE']], on='Style-Color', how='left')
smu_pricing_df = smu_items_df.join(pricing_data_df.set_index('Style-Color')[['RET_PRICE', 'A_PRICE']], on='Style-Color', how='left')





# TODO: log to seperate file
# The 'Fabric Content' column might contain non-null but empty strings or other non-null falsy values.
# Adjusting the filtering condition to handle potential:
child_production_items_df = child_production_items_df[child_production_items_df['Fabric Content'].notna() & child_production_items_df['Fabric Content'].str.strip().astype(bool)]
samples_df = samples_df[samples_df['Fabric Content'].notna() & samples_df['Fabric Content'].str.strip().astype(bool)]
sample_parent_items_df = sample_parent_items_df[sample_parent_items_df['Fabric Content'].notna() & sample_parent_items_df['Fabric Content'].str.strip().astype(bool)]
parent_items_df = parent_items_df[parent_items_df['Fabric Content'].notna() & parent_items_df['Fabric Content'].str.strip().astype(bool)]
smu_items_df = smu_items_df[smu_items_df['Fabric Content'].notna() & smu_items_df['Fabric Content'].str.strip().astype(bool)]
non_smu_items_df = non_smu_items_df[non_smu_items_df['Fabric Content'].notna() & non_smu_items_df['Fabric Content'].str.strip().astype(bool)]
smu_parent_items_df = smu_parent_items_df[smu_parent_items_df['Fabric Content'].notna() & smu_parent_items_df['Fabric Content'].str.strip().astype(bool)]
non_smu_parent_items_df = non_smu_parent_items_df[non_smu_parent_items_df['Fabric Content'].notna() & non_smu_parent_items_df['Fabric Content'].str.strip().astype(bool)]

print(f"Size of child production items DF: {len(non_smu_items_df)}")
print(f"Size of parent production items DF: {len(parent_items_df)}")

save_dataframe_to_csv(parent_items_df, base_path, multi_file_output, "upload_1_production_parent_items")
save_dataframe_to_csv(child_production_items_df, base_path, multi_file_output, "upload_2_production_children_items")
save_dataframe_to_csv(child_production_pricing_df, base_path, multi_file_output, "upload_3_production_pricing")
save_dataframe_to_csv(smu_parent_items_df, base_path, multi_file_output, "upload_4_smu_parent_items")
save_dataframe_to_csv(smu_items_df, base_path, multi_file_output, "upload_5_smu_children_items")
save_dataframe_to_csv(smu_pricing_df, base_path, multi_file_output, "upload_6_smu_pricing")
save_dataframe_to_csv(sample_parent_items_df, base_path, multi_file_output, "upload_7_sample_parent_items")
save_dataframe_to_csv(samples_df, base_path, multi_file_output, "upload_8_sample_childen_items")
save_dataframe_to_csv(sample_pricing_df, base_path, multi_file_output, "upload_9_sample_pricing_items")

output_files = [
    "vendor_purchase_prices",
    "items_with_blank_vendor",
    "upload_1_production_parent_items",
    "upload_2_production_children_items",
    "upload_3_production_pricing",
    "upload_4_smu_parent_items",
    "upload_5_smu_children_items",
    "upload_6_smu_pricing",
    "upload_7_sample_parent_items",
    "upload_8_sample_childen_items",
    "upload_9_sample_pricing_items",
    "removed_items",
    "items_with_missing_fabric_content",
    "items_with_missing_prices"
]

print("Output Files")
print("----------------")
for file in output_files:
    print(f"{base_path}{file}.csv")
