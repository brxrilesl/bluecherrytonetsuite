import pandas as pd
import sys
import numpy as np
from datetime import datetime

df = pd.read_csv("input_files/catalog_items_from_ns.csv")
sp25 = pd.read_csv("input_files/sp25_dates_sorts.csv", dtype={"Sort": int})
ho24 = pd.read_csv("input_files/ho24_dates_sorts.csv", dtype={"Sort": int})
dates_and_sorts = dates_and_sorts = pd.concat([ho24, sp25])


dates_and_sorts = pd.concat([ho24, sp25])

delivery_tag_mapping = {
    "1/25/25": "D1",
    "2/25/25": "D2",
    "3/25/25": "D3",
    "10/1/24": "D1",
    "11/1/24": "D2"
}

row_limit = 24000
drop_no_sorts = True

# dataframe column headers:
# Product Season,Product Gender,Product Category 1,External ID
#
# filter down the dataframe based on a catalogkey given as a command line input
# catalogkeys have the folowing structure {SEASON}{TWO DIGIT YEAR}{COUNTRY}{GENDER}
# NOTE: if the catalogkey's gender is "H" then both the "Product Gender" has to be "U" AND "Product Category 1" has to be "HEADWEAR"



def save_dataframe_to_csv(df, base_path, multi_file_output, file_prefix):
    if multi_file_output:
        if len(df) > row_limit:
            for i, chunk in enumerate(np.array_split(df, range(row_limit, len(df), row_limit))):
                chunk.to_csv(f"{base_path}_{file_prefix}_{i}.csv", index=False)
        else:
            df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)
    else:
        df.to_csv(f"{base_path}_{file_prefix}.csv", index=False)

def parse_catalog_key(catalog_key):
    parsed_data = {
        "season": None,
        "country": None,
        "gender": None
    }
    example_catalog_key = {
        "SPRING25USM",
        "HOLIDAY24USW",
        "SUMMER23USH",
        "FALL24USM",
        "SPRING24USW",
        "HOLIDAY25USH",
    }


    season = {
        "SPRING" : "SP",
        "SUMMER" : "SU",
        "FALL" : "FA",
        "HOLIDAY" : "HO"
    }

    country = {
        "US"
    }

    catalog_gender = {
        "M" : "M", # Mens
        "W" : "W", # Womens
        "H" : "U" # Headwear
    }

    parsed_data["season"] = season[catalog_key[:-5]] + catalog_key[-5:-3]
    parsed_data["country"] = catalog_key[-3:-1]
    parsed_data["gender"] = catalog_gender[catalog_key[-1]]

    return parsed_data

def filter_dataframe(df, catalog_key):
    parsed_data = parse_catalog_key(catalog_key)
    print(parsed_data)
    if parsed_data["gender"] == "U":
        filtered_df = df[(df["Product Season"] == parsed_data["season"]) &
                         (df["Product Category 1"] == "Headwear")]
        return filtered_df
    else:
        filtered_df = df[(df["Product Season"] == parsed_data["season"]) &
                         (df["Product Gender"] == parsed_data["gender"]) &
                         (df["Product Category 1"] == "Apparel")]
        return filtered_df

if __name__ == "__main__":
    catalog_keys = {"HOLIDAY24USH", "HOLIDAY24USM", "SPRING25USH", "SPRING25USW", "SPRING25USM"}
    all_catalogs_df = pd.DataFrame()

    for catalog_key in catalog_keys:
        filtered_df = filter_dataframe(df, catalog_key)
        filtered_df = filtered_df[["Product Gender", "Product Season", "Product Category 1", "External ID", "Subitem of"]]
        filtered_df.rename(columns={"External ID": "Item External ID"}, inplace=True)

        filtered_df["Catalog Key"] = catalog_key
        filtered_df["External ID"] = filtered_df["Item External ID"] + " - " + catalog_key

        filtered_df["Sort"] = filtered_df["Item External ID"].map(dates_and_sorts.set_index("External ID")["Sort"].to_dict())
        filtered_df["ES - Available on Date"] = filtered_df["Item External ID"].map(dates_and_sorts.set_index("External ID")["Delivery"].to_dict())

        missing_sorts_df = filtered_df[filtered_df["Sort"].isna()]
        filtered_df.dropna(subset=["Sort"], inplace=True)
        filtered_df["Sort"] = filtered_df["Sort"].astype(int)
        delivery_tag = filtered_df["ES - Available on Date"].map(delivery_tag_mapping)
        filtered_df["Product Tags"] = "Fabric Content|Gender|Product Category 1|Product Category 2|Product Category 3|Season|" + delivery_tag

        all_catalogs_df = pd.concat([all_catalogs_df, filtered_df])

        print(filtered_df)
        current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S_")
        save_dataframe_to_csv(filtered_df, "output/" + current_time, multi_file_output=True, file_prefix=catalog_key)

    # Save all catalogs combined
    current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S_")
    save_dataframe_to_csv(all_catalogs_df, "output/" + current_time, multi_file_output=True, file_prefix="all_catalogs")
