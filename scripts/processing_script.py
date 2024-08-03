import os
import sys
import pandas as pd
import yaml
import logging
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from typing import Dict, Any, List, Tuple

# Use a relative path for the output file
OUTPUT_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "processed_data.xlsx")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading YAML config: {str(e)}")
        raise

def load_and_validate_data(required_columns: List[str], input_file_path: str) -> pd.DataFrame:
    try:
        data = pd.read_excel(input_file_path, usecols=range(15))  # Read only first 15 columns
        data.columns = data.columns.str.strip()
        
        missing_columns = set(required_columns) - set(data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        return data[required_columns]  # Return only the required columns
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

def reconcile_config_with_data(config: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
    reconciled_config = {
        'master_settings': config.get('master_settings', {}),
        'task_type_filters': {},
        'custom_groups': {}
    }

    actual_task_types = data['type'].unique()
    for task_type, settings in config.get('task_type_filters', {}).items():
        if settings.get('include', False) and task_type in actual_task_types:
            reconciled_config['task_type_filters'][task_type] = settings

    for group_key, group_settings in config.get('custom_groups', {}).items():
        task_ids = group_settings.get('task_ids', [])
        valid_ids = [id for id in task_ids if id in data['Task ID'].values]
        if valid_ids:
            reconciled_config['custom_groups'][group_key] = {
                'name': group_settings.get('name', group_key),
                'task_ids': valid_ids,
                'filter_by': group_settings.get('filter_by', [])
            }
        else:
            logging.warning(f"No valid Task IDs found for group '{group_settings.get('name', group_key)}'.")

    return reconciled_config

def rename_columns(data: pd.DataFrame) -> pd.DataFrame:
    return data.rename(columns={
        'Task ID': 'Task ID',
        'Task Name': 'Task Name',
        'WBS Name': 'WBS Name',
        'Duration': 'Duration',
        'Start Date': 'Start Date',
        'End Date': 'End Date',
        'type': 'Task Type',
        'Predecessors': 'Predecessors'
    })

def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data['Start Date'] = pd.to_datetime(data['Start Date'], errors='coerce')
    data['End Date'] = pd.to_datetime(data['End Date'], errors='coerce')
    data['Duration'] = (data['End Date'] - data['Start Date']).dt.days + 1
    data['Predecessors'] = data['Predecessors'].apply(lambda x: [int(i) for i in str(x).split(',')] if pd.notna(x) else [])
    data = data.dropna(subset=['Start Date', 'End Date', 'Task Type'])
    return data

def sort_data(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data_sorted_by_start = data.sort_values(by='Start Date').reset_index(drop=True)
    data_sorted_by_end = data.sort_values(by='End Date').reset_index(drop=True)
    return data_sorted_by_start, data_sorted_by_end

def map_task_types(data: pd.DataFrame) -> pd.DataFrame:
    color_mapping = {
        'TT_FinMile': 'Background-color: #ccccff',  # Light blue for Final Milestones
        'TT_Task': 'Background-color: #ffcccc',     # Light red for Tasks
        'TT_Mile': 'Background-color: #ccffcc',     # Light green for Milestones
        'TT_LOE': 'Background-color: #ffffcc'       # Light yellow for Level of Effort (LOE)
    }
    data['Task Type Color'] = data['Task Type'].map(color_mapping)
    return data

def calculate_date_ranges(data: pd.DataFrame, date_range: str) -> Dict[str, str]:
    date_ranges = {}
    min_date = data['Start Date'].min()
    max_date = data['End Date'].max()
    
    if date_range == 'weekly':
        delta = pd.Timedelta(days=7)
    elif date_range == 'bi-monthly':
        delta = pd.Timedelta(days=14)
    elif date_range == 'monthly':
        delta = pd.Timedelta(days=30)
    elif date_range == '3months':
        delta = pd.Timedelta(days=90)
    elif date_range == '6months':
        delta = pd.Timedelta(days=180)
    else:
        delta = pd.Timedelta(days=14)  # Default to bi-monthly
    
    current_date = min_date
    period = 1
    while current_date <= max_date:
        end_date = min(current_date + delta - pd.Timedelta(days=1), max_date)
        date_ranges[f'Period {period}'] = f"{current_date.date()} to {end_date.date()}"
        current_date = end_date + pd.Timedelta(days=1)
        period += 1
    
    return date_ranges

def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=1, how='all')

def create_color_coded_df(data: pd.DataFrame, date_ranges: Dict[str, str], sort_by: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    description_dict = {}
    id_dict = {}
    color_dict = {}
    code_dict = {}

    for period, date_range in date_ranges.items():
        period_start, period_end = [pd.to_datetime(d.strip()) for d in date_range.split('to')]
        if sort_by == 'start':
            period_data = data[(data['Start Date'] >= period_start) & (data['Start Date'] <= period_end)]
        else:  # sort_by == 'end'
            period_data = data[(data['End Date'] >= period_start) & (data['End Date'] <= period_end)]
        
        descriptions = period_data['Task Name'].tolist()
        ids = period_data['Task ID'].tolist()
        colors = period_data['Task Type Color'].tolist()
        codes = period_data['task code'].tolist()

        description_dict[f'{date_range} Descriptions'] = descriptions
        id_dict[f'{date_range} IDs'] = ids
        color_dict[f'{date_range} Descriptions'] = colors
        code_dict[f'{date_range} Codes'] = codes

    description_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in description_dict.items()]))
    id_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in id_dict.items()]))
    color_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in color_dict.items()]))
    code_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in code_dict.items()]))
    
    formatted_data = pd.concat([description_df, id_df, code_df], axis=1)
    formatted_data = remove_empty_columns(formatted_data)
    color_df = color_df[formatted_data.columns[formatted_data.columns.str.contains('Descriptions')]]
    
    return formatted_data, color_df

def apply_analysis_to_filtered_data(data: pd.DataFrame, filter_config: Dict[str, Any], date_range: str) -> Tuple[Tuple[pd.DataFrame, pd.DataFrame], Tuple[pd.DataFrame, pd.DataFrame]]:
    if 'task_ids' in filter_config:
        filtered_data = data[data['Task ID'].isin(filter_config['task_ids'])]
    elif 'task_type' in filter_config:
        filtered_data = data[data['Task Type'] == filter_config['task_type']]
    else:
        filtered_data = data

    data_sorted_by_start = filtered_data.sort_values(by='Start Date').reset_index(drop=True)
    data_sorted_by_end = filtered_data.sort_values(by='End Date').reset_index(drop=True)

    data_sorted_by_start = map_task_types(data_sorted_by_start)
    data_sorted_by_end = map_task_types(data_sorted_by_end)

    date_ranges = calculate_date_ranges(filtered_data, date_range)

    formatted_data_start, color_df_start = create_color_coded_df(data_sorted_by_start, date_ranges, 'start')
    formatted_data_end, color_df_end = create_color_coded_df(data_sorted_by_end, date_ranges, 'end')

    return (formatted_data_start, color_df_start), (formatted_data_end, color_df_end)

def write_to_excel(writer: pd.ExcelWriter, formatted_data: pd.DataFrame, color_df: pd.DataFrame, sheet_name: str):
    formatted_data.to_excel(writer, index=False, sheet_name=sheet_name)
    worksheet = writer.sheets[sheet_name]
    
    for col in color_df.columns:
        for idx in range(len(color_df)):
            color = color_df.at[idx, col]
            if pd.notna(color):
                cell_format = writer.book.add_format({'bg_color': color.split(': ')[1]})
                worksheet.write(idx + 1, list(formatted_data.columns).index(col), formatted_data.at[idx, col], cell_format)

def main(config_path: str, input_file_path: str) -> str:
    required_columns = [
        'Task ID', 'Task Name', 'WBS ID', 'WBS Name', 'Duration', 
        'Start Date', 'End Date', 'Is Critical', 'Total Float', 
        '% complete', 'type', 'task code', 'resource', 'alt WBS', 'Predecessors'
    ]

    try:
        # Load configuration
        config = load_yaml_config(config_path)
        
        # Load and validate data
        data = load_and_validate_data(required_columns, input_file_path)
        
        # Reconcile configuration with actual data
        reconciled_config = reconcile_config_with_data(config, data)
        date_range = reconciled_config['master_settings'].get('date_range', 'bi-monthly')
        
        # Preprocess data
        data = rename_columns(data)
        data = clean_data(data)
        
        with pd.ExcelWriter(OUTPUT_FILE_PATH, engine='xlsxwriter') as writer:
            # Always create the two original tabs with all tasks
            (formatted_data_start_all, color_df_start_all), (formatted_data_end_all, color_df_end_all) = apply_analysis_to_filtered_data(data, {}, date_range)
            write_to_excel(writer, formatted_data_start_all, color_df_start_all, 'All_Tasks_Sorted_By_Start')
            write_to_excel(writer, formatted_data_end_all, color_df_end_all, 'All_Tasks_Sorted_By_End')
            
            # Process task type filters
            for task_type, settings in reconciled_config['task_type_filters'].items():
                filter_config = {'task_type': task_type}
                (formatted_data_start, color_df_start), (formatted_data_end, color_df_end) = apply_analysis_to_filtered_data(data, filter_config, date_range)
                
                if 'start' in settings['filter_by']:
                    write_to_excel(writer, formatted_data_start, color_df_start, f"{task_type}_start")
                if 'end' in settings['filter_by']:
                    write_to_excel(writer, formatted_data_end, color_df_end, f"{task_type}_end")
            
            # Process custom groups
            for group_key, settings in reconciled_config['custom_groups'].items():
                filter_config = {'task_ids': settings['task_ids']}
                (formatted_data_start, color_df_start), (formatted_data_end, color_df_end) = apply_analysis_to_filtered_data(data, filter_config, date_range)
                
                if 'start' in settings['filter_by']:
                    write_to_excel(writer, formatted_data_start, color_df_start, f"{settings['name']}_start")
                if 'end' in settings['filter_by']:
                    write_to_excel(writer, formatted_data_end, color_df_end, f"{settings['name']}_end")
        
        logging.info(f"Analysis completed. Output file saved at: {OUTPUT_FILE_PATH}")
        return OUTPUT_FILE_PATH
    
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python processing_script.py <config_path> <input_file_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    input_file_path = sys.argv[2]
    output_path = main(config_path, input_file_path)
    print(f"Output file saved at: {output_path}")