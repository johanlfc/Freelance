#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import csv
from datetime import datetime, timedelta

# Airtable configuration
AIRTABLE_BASE_ID = 'JOKO_BASE_ID'
TABLE_ID = 'JOKO_TABLE_ID'
API_KEY = 'AIRTABLE_API_KEY'
BASE_URL = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_ID}'

headers = {
    'Authorization': f'Bearer {API_KEY}'
}

# Mapping of fields to fetch
fields_mapping = {
    "CIO naming convention": "fldWn8OEU5wHn6vTp",
    "Channel": "fldcuNtXu0SrpLWp9",
    "Merchant IDs": "fld4a3LGvXkHf19y2",
    "Start date": "fldmBjc5EfDFOZPZp"
}

fields_to_fetch = list(fields_mapping.values())

# Date range: 30 days before today to 7 days after today
start_date = datetime.now() - timedelta(days=30)
end_date = datetime.now() + timedelta(days=7)

# Collect all records
all_records = []

# Iterate through each date in the range
current_date = start_date
while current_date <= end_date:
    # Format the date for the Airtable formula
    formatted_date = current_date.strftime('%Y-%m-%d')
    filter_formula = (
        f"AND("
        f"  IS_SAME({{fldmBjc5EfDFOZPZp}}, '{formatted_date}', 'day'),"
        f"  OR({{fldcuNtXu0SrpLWp9}} = 'Push', {{fldcuNtXu0SrpLWp9}} = 'Email'),"
        f"  {{fld4a3LGvXkHf19y2}} != ''"
        f")"
    )

    params = {
        'filterByFormula': filter_formula,
        'fields[]': fields_to_fetch
    }

    print(f"Processing date: {formatted_date}")  # Log current date being processed

    offset = None
    try:
        while True:
            if offset:
                params['offset'] = offset
            response = requests.get(BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            records = data.get('records', [])
            if records:
                print(f"  Found {len(records)} record(s) for {formatted_date}")
            else:
                print(f"  No records found for {formatted_date}")
            
            for record in records:
                # Keep fields as-is but explicitly add Record ID
                record['fields']['Record ID'] = record['id']
                all_records.append(record['fields'])  # Append the updated fields, preserving structure.
                

            offset = data.get('offset')
            if not offset:
                break
    except requests.exceptions.HTTPError as err:
        print(f"  HTTP error occurred for date {formatted_date}: {err}")
        print("  Response Text:", response.text)
    except Exception as e:
        print(f"  Unexpected error occurred for date {formatted_date}: {e}")

    # Move to the next day
    current_date += timedelta(days=1)

# Write raw fields data to a CSV
def write_raw_data_to_csv(filename, records):
    if records:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Dynamically extract all keys from the first record's fields for headers
            all_keys = set()
            for record in records:
                all_keys.update(record.keys())  # Include Record ID in the keys

            fieldnames = list(all_keys)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write each record's fields to the CSV
            for idx, record in enumerate(records):
                print(f"Writing Record {idx + 1} to CSV: {record}")  # Debugging the raw fields
                writer.writerow(record)

        print(f"Raw data has been written to {filename}")
    else:
        print(f"No records available to write to {filename}")


# Split records by channel
push_records = [record for record in all_records if record.get('Channel') == 'Push']
email_records = [record for record in all_records if record.get('Channel') == 'Email']

# Write Push and Email records to separate CSV files
write_raw_data_to_csv('push_records.csv', push_records)
write_raw_data_to_csv('email_records.csv', email_records)


# In[2]:


import csv

# Input and output file paths
input_file_push = 'push_records.csv'
filtered_file_push = 'push_records_filtered.csv'
cleaned_file_push = 'push_records_cleaned.csv'
input_file_email = 'email_records.csv'
filtered_file_email = 'email_records_filtered.csv'
cleaned_file_email = 'email_records_cleaned.csv'

# Step 1: Filter rows based on the 'Merchant IDs' column
def filter_csv(input_file, output_file):
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            # Ensure 'Merchant IDs' column exists
            if 'Merchant IDs' not in reader.fieldnames:
                print("Error: 'Merchant IDs' column not found in the input CSV.")
                return

            # Create a filtered CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()

                # Write only rows where 'Merchant IDs' does not contain a comma
                for row in reader:
                    merchant_ids = row.get('Merchant IDs', '')
                    if ',' not in merchant_ids:
                        writer.writerow(row)
                    else:
                        print(f"Skipping row with multiple Merchant IDs: {merchant_ids}")

        print(f"Filtered CSV saved as {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Step 2: Clean the 'Merchant IDs' column
def clean_csv(input_file, output_file):
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            # Ensure 'Merchant IDs' column exists
            if 'Merchant IDs' not in reader.fieldnames:
                print("Error: 'Merchant IDs' column not found in the input CSV.")
                return

            # Create a cleaned CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()

                # Clean the 'Merchant IDs' column
                for row in reader:
                    merchant_ids = row.get('Merchant IDs', '')
                    # Remove unwanted characters
                    cleaned_ids = merchant_ids.replace('[', '').replace(']', '').replace("'", '').strip()
                    row['Merchant IDs'] = cleaned_ids
                    writer.writerow(row)

        print(f"Cleaned CSV saved as {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the functions for push
filter_csv(input_file_push, filtered_file_push)
clean_csv(filtered_file_push, cleaned_file_push)

# Run the functions for email
filter_csv(input_file_email, filtered_file_email)
clean_csv(filtered_file_email, cleaned_file_email)


# In[3]:


import csv
import pandas as pd
from datetime import datetime, timedelta

# Input and output file paths
input_file_push = 'push_records_cleaned.csv'
output_file_push = 'push_records_with_segments.csv'
input_file_email = 'email_records_cleaned.csv'
output_file_email = 'email_records_with_segments.csv'

def process_csv_email(input_file, output_file):
    try:
        # Load all rows into memory
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            rows = list(reader)

            # Ensure required columns exist
            required_columns = ['Start date', 'Customer.io naming convention', 'Merchant IDs']
            for col in required_columns:
                if col not in reader.fieldnames:
                    print(f"Error: '{col}' column not found in the input CSV.")
                    return

            # Add the new column to the fieldnames
            fieldnames = reader.fieldnames + ['Segments to exclude']

        # Process rows and add the new column
        for row in rows:
            current_start_date = datetime.strptime(row['Start date'], '%Y-%m-%d')
            merchant_ids = row['Merchant IDs']
            cio_naming_convention = row['Customer.io naming convention']

            # Only process rows with a Start date equal to or after today
            if current_start_date >= datetime.now():
                # Find rows with Start date in the last 30 days
                segments_to_exclude = []
                for other_row in rows:
                    other_start_date = datetime.strptime(other_row['Start date'], '%Y-%m-%d')
                    if (
                        other_start_date >= current_start_date - timedelta(days=30)
                        and other_start_date < current_start_date
                    ):
                        # Check if CIO naming convention contains the Merchant IDs
                        if merchant_ids in other_row['Customer.io naming convention']:
                            segments_to_exclude.append(other_row['Customer.io naming convention'])

                # Join matching CIO naming conventions with ///
                row['Segments to exclude'] = '\n'.join(segments_to_exclude)
            else:
                # Leave the column empty if Start date is before today
                row['Segments to exclude'] = ''

        # Write the updated rows to the output file
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Processed CSV email saved as {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
def process_csv_push(input_file, output_file):
    try:
        # Load all rows into memory
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            rows = list(reader)

            # Ensure required columns exist
            required_columns = ['Start date', 'Customer.io naming convention', 'Merchant IDs']
            for col in required_columns:
                if col not in reader.fieldnames:
                    print(f"Error: '{col}' column not found in the input CSV.")
                    return

            # Add the new column to the fieldnames
            fieldnames = reader.fieldnames + ['Segments to exclude']

        # Process rows and add the new column
        for row in rows:
            current_start_date = datetime.strptime(row['Start date'], '%Y-%m-%d')
            merchant_ids = row['Merchant IDs']
            cio_naming_convention = row['Customer.io naming convention']

            # Only process rows with a Start date equal to or after today
            if current_start_date >= datetime.now():
                # Find rows with Start date in the last 30 days
                segments_to_exclude = []
                for other_row in rows:
                    other_start_date = datetime.strptime(other_row['Start date'], '%Y-%m-%d')
                    if (
                        other_start_date >= current_start_date - timedelta(days=30)
                        and other_start_date < current_start_date
                    ):
                        # Check if CIO naming convention contains the Merchant IDs
                        if merchant_ids in other_row['Customer.io naming convention']:
                            segments_to_exclude.append(f"{other_row['Customer.io naming convention']}_Push_SDK_Version")

                # Join matching CIO naming conventions with ///
                row['Segments to exclude'] = '\n'.join(segments_to_exclude)
            else:
                # Leave the column empty if Start date is before today
                row['Segments to exclude'] = ''

        # Write the updated rows to the output file
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Processed CSV push saved as {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        

# Run the function
process_csv_push(input_file_push, output_file_push)
process_csv_email(input_file_email, output_file_email)


# In[4]:


import csv

# Input and output file paths
email_input_file = 'email_records_with_segments.csv'
push_input_file = 'push_records_with_segments.csv'
email_output_file = 'filtered_email_records.csv'
push_output_file = 'filtered_push_records.csv'

def filter_rows_with_segments(input_file, output_file):
    try:
        # Initialize list to store filtered rows
        filtered_rows = []

        # Open the input file
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            # Ensure 'Segments to exclude' column exists
            if 'Segments to exclude' not in reader.fieldnames:
                print(f"Error: 'Segments to exclude' column not found in {input_file}.")
                return

            # Filter rows where 'Segments to exclude' is not empty
            for row in reader:
                if row['Segments to exclude'].strip():  # Check if not empty
                    filtered_rows.append(row)

        # Write filtered rows to the new CSV file
        if filtered_rows:
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                fieldnames = filtered_rows[0].keys()  # Use keys from the first row for headers
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(filtered_rows)

            print(f"Filtered data has been written to {output_file}")
        else:
            print(f"No rows with non-empty 'Segments to exclude' found in {input_file}.")
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the function for both email and push files
filter_rows_with_segments(email_input_file, email_output_file)
filter_rows_with_segments(push_input_file, push_output_file)


# In[5]:


import csv
import requests
from datetime import datetime

# Airtable configuration
AIRTABLE_BASE_ID = 'JOKO_BASE_ID'
TABLE_ID = 'JOKO_TABLE_ID'
API_KEY = 'AIRTABLE_API_KEY'
AIRTABLE_URL = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{TABLE_ID}'

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# Input file
input_file_email = 'filtered_email_records.csv'
input_file_push = 'filtered_push_records.csv'


def update_airtable(input_file):
    try:
        # Read the CSV file
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            rows = list(reader)

            # Ensure required columns exist
            required_columns = ['Record ID', 'Segments to exclude']
            for col in required_columns:
                if col not in reader.fieldnames:
                    print(f"Error: '{col}' column not found in the input CSV.")
                    return

            # Current date for filtering
            today = datetime.now().date()

            # Process rows
            for row in rows:
                record_id = row['Record ID']
                segments_to_exclude = row['Segments to exclude']
                # Prepare data for Airtable
                payload = {
                    "fields": {
                           "fldZKBTFw4WUEUNqu": segments_to_exclude
                     }
                }

                # Make API request to update Airtable
                url = f"{AIRTABLE_URL}/{record_id}"
                response = requests.patch(url, headers=headers, json=payload)

                # Log the result
                if response.status_code == 200:
                    print(f"Record {record_id} updated successfully.")
                else:
                    print(f"Failed to update Record {record_id}: {response.status_code}")
                    print(f"Response: {response.text}")

        print("Airtable update process completed.")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the function
update_airtable(input_file_email)
update_airtable(input_file_push)

