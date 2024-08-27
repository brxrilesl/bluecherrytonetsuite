# Project Overview

This repository contains several scripts designed to process and transform product and inventory data for integration with an inventory management system. Below is a summary of each script and its functionality.

## Installation and Setup

Before executing any scripts, follow these steps to prepare your environment:

1. **Python Installation**: Ensure Python 3.6 or later is installed on your system. You can download it from [https://python.org](https://python.org).

2. **Virtual Environment Setup**:
   - Open a terminal or command prompt.
   - Navigate to the project directory.
   - Run `python -m venv .venv` to create a virtual environment named `.venv`.
   - Activate the virtual environment:
     - On Windows, execute `.venv\Scripts\activate`.
     - On macOS and Linux, execute `source .venv/bin/activate`.

3. **Install Required Packages**:
   - Ensure a file named `requirements.txt` exists in your project directory with the following contents:
     ```
     pandas>=1.1
     numpy>=1.19
     datetime
     ```
   - With your virtual environment activated, install the required packages by running `pip install -r requirements.txt`.

## Script Descriptions

### main.py

This script processes product data to prepare it for upload into an inventory management system. Key operations include:

- **Vendor Remapping**: Remaps vendor codes based on a predefined mapping.
- **Data Cleaning**: Removes rows with all NaN values.
- **Category Mapping**: Maps product categories using an external CSV file.
- **Child and Parent Item Creation**: Generates DataFrames for child and parent items, including size and category information.
- **Output Generation**: Produces CSV files for parent items, child items, and pricing data.

### inventory_quantities.py

This script processes inventory quantity data from a CSV file. Key operations include:

- **Location ID Lookup**: Maps location names to internal IDs.
- **Data Mapping**: Maps values from the input DataFrame to a new output DataFrame.
- **Output Generation**: Saves the processed data to a new CSV file.

### current_inv.py

This script processes current inventory data from multiple Excel files. Key operations include:

- **File Reading**: Reads Excel files from a specified directory.
- **Data Transformation**: Adds location and memo columns, and generates external IDs.
- **Output Generation**: Saves each processed DataFrame to a separate CSV file based on location.

### costrevals.py

This script processes cost revaluation data. Key operations include:

- **Data Loading**: Reads multiple CSV files containing cost revaluation and item data.
- **Data Mapping**: Maps parent item costs to child items and locations.
- **Output Generation**: Produces CSV files for cost revaluation uploads.

### errors.py

This script processes error logs from multiple CSV files. Key operations include:

- **File Reading**: Reads all CSV files in the `results` directory.
- **Data Concatenation**: Combines the data into a single DataFrame.
- **Duplicate Removal**: Removes duplicate error entries.
- **Output Generation**: Saves the combined error data to a new CSV file.

## Usage Instructions

1. Place the input files in the appropriate directories as expected by each script.
2. Ensure your virtual environment is activated.
3. Execute the desired script using Python:
   ```
   python script_name.py
   ```
4. Check the `output/` directory for the generated CSV files.

## Important Notes

- The scripts assume the presence of specific columns in the input files. Adjustments may be required if your file structure differs.
- Always review the output files to ensure data accuracy before proceeding with any uploads to the inventory management system.
