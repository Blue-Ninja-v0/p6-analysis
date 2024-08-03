import os
import sys
import logging
from datetime import datetime

# Get the absolute path of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the parent directory to sys.path
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Now import the scripts
from scripts import script1, script2, script3, script4, processing_script

def setup_logging(log_file):
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def create_output_directory(input_file_path):
    base_dir = os.path.dirname(input_file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(base_dir, f"processed_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def process_xer_file(input_file_path, progress_info):
    output_dir = create_output_directory(input_file_path)
    log_file = os.path.join(output_dir, 'processing.log')
    setup_logging(log_file)
    
    output_files = []
    
    try:
        # Run Script 1: XER File Parsing
        progress_info['current_step'] = "XER File Parsing"
        progress_info['step_progress'] = 0
        progress_info['overall_progress'] = 0
        logging.info("Starting Script 1: XER File Parsing")
        parsed_data_path = script1.parse_and_save_raw_data(input_file_path, output_dir)
        output_files.append(("Parsed Raw Data", os.path.relpath(parsed_data_path, start=os.path.dirname(input_file_path))))
        logging.info(f"Script 1 completed. Output: {parsed_data_path}")
        progress_info['step_progress'] = 100
        progress_info['overall_progress'] = 25

        # Run Script 2: Data Preprocessing and Analysis
        progress_info['current_step'] = "Data Preprocessing and Analysis"
        progress_info['step_progress'] = 0
        logging.info("Starting Script 2: Data Preprocessing and Analysis")
        combined_output_path = script2.process_parsed_data(parsed_data_path, output_dir)
        output_files.append(("Combined Output", os.path.relpath(combined_output_path, start=os.path.dirname(input_file_path))))
        logging.info(f"Script 2 completed. Output: {combined_output_path}")
        progress_info['step_progress'] = 100
        progress_info['overall_progress'] = 50

        # Run Script 3: Critical Path Analysis and Comprehensive Reporting
        progress_info['current_step'] = "Critical Path Analysis and Reporting"
        progress_info['step_progress'] = 0
        logging.info("Starting Script 3: Critical Path Analysis and Comprehensive Reporting")
        comprehensive_report_path = script3.main(combined_output_path, output_dir)
        output_files.append(("Comprehensive Report", os.path.relpath(comprehensive_report_path, start=os.path.dirname(input_file_path))))
        logging.info(f"Script 3 completed. Output: {comprehensive_report_path}")
        progress_info['step_progress'] = 100
        progress_info['overall_progress'] = 75

        # Run Script 4: Gantt Chart Generation
        progress_info['current_step'] = "Gantt Chart Generation"
        progress_info['step_progress'] = 0
        logging.info("Starting Script 4: Gantt Chart Generation")
        gantt_chart_path = script4.main(comprehensive_report_path, output_dir)
        output_files.append(("Gantt Chart", os.path.relpath(gantt_chart_path, start=os.path.dirname(input_file_path))))
        logging.info(f"Script 4 completed. Output: {gantt_chart_path}")
        progress_info['step_progress'] = 100
        progress_info['overall_progress'] = 100

        output_files.append(("Processing Log", os.path.relpath(log_file, start=os.path.dirname(input_file_path))))
        return output_files, gantt_chart_path

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

def run_final_analysis(config_path, gantt_chart_path):
    try:
        # Ensure the gantt_chart_path is an absolute path
        gantt_chart_path = os.path.abspath(gantt_chart_path)
        
        # Check if the file exists
        if not os.path.exists(gantt_chart_path):
            raise FileNotFoundError(f"Gantt chart file not found at {gantt_chart_path}")
        
        output_path = processing_script.main(config_path, gantt_chart_path)
        return output_path
    except Exception as e:
        logging.error(f"An error occurred during final analysis: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing purposes
    input_file = "/path/to/your/input.xer"
    config_path = os.path.join(current_dir, "config.yaml")
    results, gantt_chart_path = process_xer_file(input_file)
    for name, path in results:
        print(f"{name}: {path}")
    
    final_output = run_final_analysis(config_path, gantt_chart_path)
    print(f"Final analysis output: {final_output}")