import pandas as pd
import os
import numpy as np
import logging

def ensure_string_columns(*dfs, columns):
    """Ensure the specified columns in the given DataFrames are of string type."""
    for df in dfs:
        for column in columns:
            if column in df.columns:
                df[column] = df[column].astype(str)
    return dfs

def calculate_working_days(start_date, end_date, calendar_id, calendar_df):
    calendar = calendar_df[calendar_df['clndr_id'] == calendar_id].iloc[0]
    total_days = (end_date - start_date).days + 1
    weeks, remaining_days = divmod(total_days, 7)
    
    working_days = weeks * (calendar['week_hr_cnt'] / calendar['day_hr_cnt'])
    working_days += min(remaining_days, 5)  # Assume at most 5 working days in the remaining days
    
    return working_days

def preprocess_data_frames(task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df):
    date_columns = [
        'target_start_date', 'target_end_date',
        'early_start_date', 'early_end_date',
        'act_start_date', 'act_end_date'
    ]
    
    # Convert date columns to datetime
    for df in [task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df]:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
    # Ensure the correct data types for merging
    ensure_string_columns(
        task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df,
        columns=['task_id', 'wbs_id', 'rsrc_id', 'pred_task_id', 'clndr_id', 'fk_id', 'actv_code_id']
    )
    
    return task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df

def prepare_and_merge_data_v2(task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df):
    task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df = preprocess_data_frames(
        task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df
    )

    # Merge WBS data
    enhanced_df = task_df.merge(projwbs_df[['wbs_id', 'wbs_short_name', 'wbs_name']], on='wbs_id', how='left')

    # Merge resource assignments
    task_rsrc_df = taskrsrc_df.merge(rsrc_df[['rsrc_id', 'rsrc_short_name', 'rsrc_name']], on='rsrc_id', how='left')
    task_rsrc_df['rsrc_name'] = task_rsrc_df['rsrc_name'].fillna('').astype(str)
    resource_assignments = task_rsrc_df.groupby('task_id').agg({'rsrc_name': lambda x: ', '.join(x)}).reset_index()
    enhanced_df = enhanced_df.merge(resource_assignments, on='task_id', how='left')

    # Merge task dependencies
    task_dependencies = taskpred_df.groupby('task_id').agg({'pred_task_id': lambda x: ', '.join(x)}).reset_index()
    enhanced_df = enhanced_df.merge(task_dependencies, on='task_id', how='left')

    # Merge calendar data
    enhanced_df = enhanced_df.merge(calendar_df[['clndr_id', 'clndr_name']], on='clndr_id', how='left')

    # Merge UDF fields
    udfvalue_df['udf_text'] = udfvalue_df['udf_text'].astype(str)
    udf_fields = udfvalue_df.groupby('fk_id').agg({'udf_text': lambda x: ', '.join(x)}).reset_index().rename(columns={'fk_id': 'task_id'})
    enhanced_df = enhanced_df.merge(udf_fields, on='task_id', how='left')

    # Merge activity codes
    task_actvcode_df = taskactv_df.merge(actvcode_df[['actv_code_id', 'actv_code_name']], on='actv_code_id', how='left')
    activity_codes = task_actvcode_df.groupby('task_id').agg({'actv_code_name': lambda x: ', '.join(map(str, x))}).reset_index()
    enhanced_df = enhanced_df.merge(activity_codes, on='task_id', how='left')

    # Calculate durations
    enhanced_df['planned_duration'] = enhanced_df.apply(lambda row: calculate_working_days(
        row['target_start_date'], row['target_end_date'], row['clndr_id'], calendar_df
    ), axis=1)
    
    enhanced_df['actual_duration'] = np.where(
        enhanced_df['act_start_date'].notna() & enhanced_df['act_end_date'].notna(),
        enhanced_df.apply(lambda row: calculate_working_days(
            row['act_start_date'], row['act_end_date'], row['clndr_id'], calendar_df
        ), axis=1),
        np.nan
    )

    return enhanced_df

def calculate_key_metrics(enhanced_df):
    enhanced_df['start_date_variance'] = (enhanced_df['act_start_date'] - enhanced_df['target_start_date']).dt.days
    enhanced_df['end_date_variance'] = (enhanced_df['act_end_date'] - enhanced_df['target_end_date']).dt.days

    missing_resources = enhanced_df[enhanced_df['rsrc_name'].isna()]
    missing_dependencies = enhanced_df[enhanced_df['pred_task_id'].isna()]
    missing_udf = enhanced_df[enhanced_df['udf_text'].isna()]
    missing_activity_codes = enhanced_df[enhanced_df['actv_code_name'].isna()]

    return {
        'enhanced_df': enhanced_df,
        'missing_resources': missing_resources,
        'missing_dependencies': missing_dependencies,
        'missing_udf': missing_udf,
        'missing_activity_codes': missing_activity_codes
    }

def prepare_calendar_data(calendar_df):
    calendar_data = calendar_df[['clndr_id', 'clndr_name', 'day_hr_cnt', 'week_hr_cnt', 'month_hr_cnt', 'year_hr_cnt']]
    return calendar_data

def process_parsed_data(excel_path, output_dir):
    logging.info(f"Starting to process parsed data from: {excel_path}")
    
    combined_output_path = os.path.join(output_dir, 'combined_output.xlsx')
    
    with pd.ExcelFile(excel_path) as xls:
        task_df = pd.read_excel(xls, 'TASK')
        projwbs_df = pd.read_excel(xls, 'PROJWBS')
        rsrc_df = pd.read_excel(xls, 'RSRC')
        taskrsrc_df = pd.read_excel(xls, 'TASKRSRC')
        taskpred_df = pd.read_excel(xls, 'TASKPRED')
        calendar_df = pd.read_excel(xls, 'CALENDAR')
        udfvalue_df = pd.read_excel(xls, 'UDFVALUE')
        taskactv_df = pd.read_excel(xls, 'TASKACTV')
        actvcode_df = pd.read_excel(xls, 'ACTVCODE')

    logging.info("All required sheets loaded successfully")

    enhanced_merged_df = prepare_and_merge_data_v2(task_df, projwbs_df, rsrc_df, taskrsrc_df, taskpred_df, calendar_df, udfvalue_df, taskactv_df, actvcode_df)
    logging.info("Data preparation and merging completed")

    calendar_data = prepare_calendar_data(calendar_df)
    logging.info("Calendar data prepared")

    metrics = calculate_key_metrics(enhanced_merged_df)
    logging.info("Key metrics calculated")

    with pd.ExcelWriter(combined_output_path) as writer:
        metrics['enhanced_df'].to_excel(writer, sheet_name='Enhanced Baseline Data', index=False)
        metrics['missing_resources'].to_excel(writer, sheet_name='Missing Resources', index=False)
        metrics['missing_dependencies'].to_excel(writer, sheet_name='Missing Dependencies', index=False)
        metrics['missing_udf'].to_excel(writer, sheet_name='Missing UDF', index=False)
        metrics['missing_activity_codes'].to_excel(writer, sheet_name='Missing Activity Codes', index=False)
        calendar_data.to_excel(writer, sheet_name='Calendar Data', index=False)
    
    logging.info(f"Combined output saved to: {combined_output_path}")
    
    return combined_output_path

# Remove the example usage from the script