import pandas as pd
import os
import logging

def load_xer_file(file_path):
    try:
        with open(file_path, 'r', encoding='latin-1') as file:
            xer_content = file.read()
        return xer_content
    except Exception as e:
        logging.error(f"Error reading XER file: {e}")
        raise

def parse_xer_content(xer_content):
    lines = xer_content.split('\n')
    tables = {}
    current_table = None
    fields = []
    for line in lines:
        if line.startswith('%T'):
            current_table = line.split('\t')[1]
            tables[current_table] = {'fields': [], 'records': []}
        elif line.startswith('%F'):
            fields = line.split('\t')[1:]
            tables[current_table]['fields'] = fields
        elif line.startswith('%R') and current_table:
            record = line.split('\t')[1:]
            tables[current_table]['records'].append(record)
    return tables

def save_parsed_data_to_excel(tables, output_path):
    with pd.ExcelWriter(output_path) as writer:
        for table_name, data in tables.items():
            df = pd.DataFrame(data['records'], columns=data['fields'])
            df.to_excel(writer, sheet_name=table_name, index=False)
    logging.info(f"Saved all tables to {output_path}")

def parse_and_save_raw_data(file_path, output_dir):
    logging.info(f"Starting to parse XER file: {file_path}")
    xer_content = load_xer_file(file_path)
    logging.info("XER file loaded successfully")
    
    tables = parse_xer_content(xer_content)
    logging.info("XER content parsed successfully")
    
    input_filename = os.path.splitext(os.path.basename(file_path))[0]
    output_path = os.path.join(output_dir, f"{input_filename}_parsed_data.xlsx")
    
    save_parsed_data_to_excel(tables, output_path)
    logging.info(f"Parsed data saved to: {output_path}")
    
    return output_path

# Remove the example usage from the script