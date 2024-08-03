import pandas as pd
import numpy as np
from datetime import timedelta
import concurrent.futures
from tqdm import tqdm
import os
import logging

def load_combined_data(combined_output_path):
    with pd.ExcelFile(combined_output_path) as xls:
        enhanced_df = pd.read_excel(xls, 'Enhanced Baseline Data')
        calendar_df = pd.read_excel(xls, 'Calendar Data')
    return enhanced_df, calendar_df

def calculate_working_days(start_date, end_date, calendar_id, calendar_df):
    calendar = calendar_df[calendar_df['clndr_id'] == calendar_id].iloc[0]
    total_days = (end_date - start_date).days + 1
    weeks, remaining_days = divmod(total_days, 7)
    
    working_days = weeks * (calendar['week_hr_cnt'] / calendar['day_hr_cnt'])
    working_days += min(remaining_days, 5)  # Assume at most 5 working days in the remaining days
    
    return working_days

def prepare_data(df, calendar_df):
    df['target_start_date'] = pd.to_datetime(df['target_start_date'], errors='coerce')
    df['target_end_date'] = pd.to_datetime(df['target_end_date'], errors='coerce')
    df['act_start_date'] = pd.to_datetime(df['act_start_date'], errors='coerce')
    df['act_end_date'] = pd.to_datetime(df['act_end_date'], errors='coerce')
    
    df['planned_duration'] = df.apply(lambda row: calculate_working_days(
        row['target_start_date'], row['target_end_date'], row['clndr_id'], calendar_df
    ), axis=1)
    
    df['actual_duration'] = np.where(
        df['act_start_date'].notna() & df['act_end_date'].notna(),
        df.apply(lambda row: calculate_working_days(
            row['act_start_date'], row['act_end_date'], row['clndr_id'], calendar_df
        ), axis=1),
        np.nan
    )
    
    return df

def create_comprehensive_excel_report(enhanced_df, calendar_df, output_path):
    logging.info("Creating comprehensive Excel report")
    with tqdm(total=100, desc="Creating Comprehensive Excel Output") as pbar:
        prepared_df = prepare_data(enhanced_df, calendar_df)
        pbar.update(20)
        logging.info("Data preparation completed")

        prepared_df['start_date_variance'] = (prepared_df['act_start_date'] - prepared_df['target_start_date']).dt.days
        prepared_df['end_date_variance'] = (prepared_df['act_end_date'] - prepared_df['target_end_date']).dt.days
        pbar.update(20)
        logging.info("Date variance calculations completed")

        prepared_df['total_float_hr_cnt'] = pd.to_numeric(prepared_df['total_float_hr_cnt'], errors='coerce')
        simple_critical_activities = prepared_df[prepared_df['total_float_hr_cnt'] <= 0]
        pbar.update(20)
        logging.info("Simple critical activities identified")

        wbs_agg = prepared_df.groupby(['wbs_id', 'wbs_short_name', 'wbs_name']).agg({
            'planned_duration': 'sum',
            'actual_duration': 'sum',
            'start_date_variance': 'mean',
            'end_date_variance': 'mean'
        }).reset_index()
        pbar.update(20)
        logging.info("WBS aggregation completed")

        # Critical Path Analysis
        results_df, critical_path = perform_critical_path_analysis(prepared_df)
        pbar.update(10)
        logging.info("Critical path analysis completed")

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            prepared_df.to_excel(writer, sheet_name='Enhanced Baseline Data', index=False)
            simple_critical_activities.to_excel(writer, sheet_name='Simple Critical Activities', index=False)
            wbs_agg.to_excel(writer, sheet_name='WBS Aggregated Data', index=False)
            calendar_df.to_excel(writer, sheet_name='Calendar Data', index=False)
            results_df.to_excel(writer, sheet_name='Critical Path Analysis', index=False)
            
            prepared_df[prepared_df['rsrc_name'].isna()].to_excel(writer, sheet_name='Missing Resources', index=False)
            prepared_df[prepared_df['pred_task_id'].isna()].to_excel(writer, sheet_name='Missing Dependencies', index=False)
            prepared_df[prepared_df['udf_text'].isna()].to_excel(writer, sheet_name='Missing UDF', index=False)
            prepared_df[prepared_df['actv_code_name'].isna()].to_excel(writer, sheet_name='Missing Activity Codes', index=False)
            
            # Highlight critical tasks
            workbook = writer.book
            worksheet = writer.sheets['Critical Path Analysis']
            critical_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            
            for row, is_critical in enumerate(results_df['is_critical'], start=1):
                if is_critical:
                    worksheet.set_row(row, None, critical_format)
        
        pbar.update(10)
        logging.info(f"Comprehensive Excel report saved to: {output_path}")

class Task:
    def __init__(self, task_id, name, planned_start, planned_finish, duration, predecessors):
        self.task_id = task_id
        self.name = name
        self.planned_start = planned_start
        self.planned_finish = planned_finish
        self.duration = duration
        self.predecessors = predecessors
        self.successors = []
        self.early_start = None
        self.early_finish = None
        self.late_start = None
        self.late_finish = None
        self.total_float = None
        self.free_float = None
        self.is_critical = False

def create_task_objects(df):
    tasks = {}
    for _, row in df.iterrows():
        task = Task(
            row['task_id'],
            row['task_name'],
            row['target_start_date'],
            row['target_end_date'],
            row['planned_duration'],
            row['pred_task_id'].split(', ') if pd.notna(row['pred_task_id']) else []
        )
        tasks[task.task_id] = task
    
    for task in tasks.values():
        for pred_id in task.predecessors:
            if pred_id in tasks:
                tasks[pred_id].successors.append(task.task_id)
    
    return tasks

def forward_pass(tasks):
    def process_task(task):
        if task.early_start is not None:
            return
        
        if not task.predecessors:
            task.early_start = task.planned_start
        else:
            pred_finish_dates = [tasks[pred_id].early_finish for pred_id in task.predecessors if pred_id in tasks and tasks[pred_id].early_finish]
            if pred_finish_dates:
                task.early_start = max(pred_finish_dates)
        
        if task.early_start:
            task.early_finish = task.early_start + timedelta(days=task.duration)
        
        for succ_id in task.successors:
            if succ_id in tasks:
                process_task(tasks[succ_id])

    for task in tasks.values():
        if not task.predecessors:
            process_task(task)

def backward_pass(tasks):
    def process_task(task):
        if task.late_finish is not None:
            return
        
        if not task.successors:
            task.late_finish = task.early_finish
        else:
            succ_start_dates = [tasks[succ_id].late_start for succ_id in task.successors if succ_id in tasks and tasks[succ_id].late_start]
            if succ_start_dates:
                task.late_finish = min(succ_start_dates)
            else:
                task.late_finish = task.early_finish
        
        if task.late_finish:
            task.late_start = task.late_finish - timedelta(days=task.duration)
        
        for pred_id in task.predecessors:
            if pred_id in tasks:
                process_task(tasks[pred_id])

    end_tasks = [task for task in tasks.values() if not task.successors]
    
    if not end_tasks:
        latest_task = max(tasks.values(), key=lambda t: t.early_finish)
        latest_task.late_finish = latest_task.early_finish
        end_tasks = [latest_task]

    for task in end_tasks:
        process_task(task)

def calculate_floats_and_critical_path(tasks):
    critical_path = []
    for task in tasks.values():
        if task.early_start and task.late_start:
            task.total_float = (task.late_start - task.early_start).days
            task.is_critical = task.total_float == 0
            if task.is_critical:
                critical_path.append(task.task_id)
        
        if task.successors:
            task.free_float = min((tasks[succ_id].early_start - task.early_finish).days for succ_id in task.successors if succ_id in tasks)
        else:
            task.free_float = task.total_float
    
    return critical_path

def perform_critical_path_analysis(df):
    logging.info("Performing critical path analysis")
    tasks = create_task_objects(df)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(forward_pass, tasks)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(backward_pass, tasks)
    
    problematic_tasks = [task for task in tasks.values() if task.early_start is None or task.late_start is None]
    if problematic_tasks:
        logging.warning("Some tasks have undefined early or late start dates:")
        for task in problematic_tasks:
            logging.warning(f"Task ID: {task.task_id}, Name: {task.name}")
    
    critical_path = calculate_floats_and_critical_path(tasks)
    
    results = []
    for task in tasks.values():
        results.append({
            'task_id': task.task_id,
            'task_name': task.name,
            'early_start': task.early_start,
            'early_finish': task.early_finish,
            'late_start': task.late_start,
            'late_finish': task.late_finish,
            'total_float': task.total_float,
            'free_float': task.free_float,
            'is_critical': task.is_critical
        })
    
    results_df = pd.DataFrame(results)
    logging.info("Critical path analysis completed")
    
    return results_df, critical_path

def main(combined_output_path, output_dir):
    logging.info(f"Starting script3 with input: {combined_output_path}")
    enhanced_df, calendar_df = load_combined_data(combined_output_path)
    logging.info("Data loaded successfully")
    
    input_filename = os.path.splitext(os.path.basename(combined_output_path))[0]
    comprehensive_excel_path = os.path.join(output_dir, f"{input_filename}_Comprehensive_Report.xlsx")

    create_comprehensive_excel_report(enhanced_df, calendar_df, comprehensive_excel_path)

    logging.info("Script3 completed successfully")
    return comprehensive_excel_path

if __name__ == "__main__":
    # This block will not be executed when imported as a module
    print("This script is designed to be run as part of a larger pipeline.")
    print("Please use the orchestrator to run the full analysis.")