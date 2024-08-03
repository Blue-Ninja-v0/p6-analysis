import os
import threading
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
import yaml
from orchestrator import process_xer_file, run_final_analysis as orchestrator_run_final_analysis
import logging

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Replace with a real secret key

progress_info = {'overall_progress': 0, 'step_progress': 0, 'current_step': 'Initializing...'}

# Use relative paths
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
ALLOWED_EXTENSIONS = {'xer'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

logging.basicConfig(level=logging.DEBUG)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_yaml_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def save_yaml_config(config_path, config):
    with open(config_path, 'w') as file:
        yaml.dump(config, file)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part', 'error')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            return redirect(url_for('processing', filename=filename))
    return render_template('upload.html')

@app.route('/processing/<filename>')
def processing(filename):
    threading.Thread(target=process_file_background, args=(filename,)).start()
    return render_template('processing.html', filename=filename)

@app.route('/progress')
def get_progress():
    return jsonify(progress_info)

def process_file_background(filename):
    global progress_info
    input_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    try:
        output_files, gantt_chart_path = process_xer_file(input_path, progress_info)
        progress_info['overall_progress'] = 100
        progress_info['current_step'] = 'Complete'
    except Exception as e:
        app.logger.error(f"Error processing file: {str(e)}")
        progress_info['current_step'] = f"Error: {str(e)}"

@app.route('/process/<filename>')
def process_file(filename):
    global progress_info
    input_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    try:
        output_files, gantt_chart_path = process_xer_file(input_path, progress_info)
        relative_gantt_chart_path = os.path.relpath(gantt_chart_path, start=app.config['UPLOAD_FOLDER'])
        return render_template('download.html', output_files=output_files, gantt_chart_path=relative_gantt_chart_path)
    except Exception as e:
        app.logger.error(f"Error processing file: {str(e)}")
        flash(f"An error occurred while processing the file: {str(e)}", 'error')
        return redirect(url_for('upload_file'))

@app.route('/download/<path:filename>')
def download_file(filename):
    if filename.startswith(app.config['UPLOAD_FOLDER']):
        filename = filename[len(app.config['UPLOAD_FOLDER']):].lstrip('/')
    
    full_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if not os.path.commonprefix([os.path.realpath(full_path), app.config['UPLOAD_FOLDER']]) == app.config['UPLOAD_FOLDER']:
        flash("Access denied", 'error')
        return redirect(url_for('upload_file'))
    
    return send_file(full_path, as_attachment=True, download_name=filename)

@app.route('/configure_analysis/<path:gantt_chart_path>', methods=['GET', 'POST'])
def configure_analysis(gantt_chart_path):
    config = load_yaml_config(CONFIG_PATH)
    if request.method == 'POST':
        action = request.form.get('action')
        update_config(config, request.form)
        save_yaml_config(CONFIG_PATH, config)
        
        if action == 'save':
            flash('Configuration saved successfully!', 'success')
            return redirect(url_for('configure_analysis', gantt_chart_path=gantt_chart_path))
        elif action == 'run':
            return redirect(url_for('run_final_analysis_route', gantt_chart_path=gantt_chart_path))
    
    return render_template('configure_analysis.html', config=config, gantt_chart_path=gantt_chart_path)

@app.route('/run_final_analysis/<path:gantt_chart_path>', methods=['GET', 'POST'])
def run_final_analysis_route(gantt_chart_path):
    try:
        if gantt_chart_path.startswith(app.config['UPLOAD_FOLDER']):
            gantt_chart_path = gantt_chart_path[len(app.config['UPLOAD_FOLDER']):].lstrip('/')
        
        full_gantt_chart_path = os.path.join(app.config['UPLOAD_FOLDER'], gantt_chart_path)
        
        if not os.path.exists(full_gantt_chart_path):
            raise FileNotFoundError(f"Gantt chart file not found at {full_gantt_chart_path}")
        
        app.logger.debug(f"Running final analysis with gantt chart: {full_gantt_chart_path}")
        output_path = orchestrator_run_final_analysis(CONFIG_PATH, full_gantt_chart_path)
        app.logger.debug(f"Analysis complete. Output file: {output_path}")
        
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file not found at {output_path}")
        
        return send_file(
            output_path,
            as_attachment=True,
            download_name=os.path.basename(output_path),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        app.logger.error(f"Error in final analysis: {str(e)}")
        flash(f"An error occurred during the final analysis: {str(e)}", 'error')
        return redirect(url_for('configure_analysis', gantt_chart_path=gantt_chart_path))

def update_config(config, form_data):
    config['master_settings']['date_range'] = form_data['date_range']
    
    for task_type in config['task_type_filters']:
        config['task_type_filters'][task_type]['include'] = task_type in form_data
        config['task_type_filters'][task_type]['filter_by'] = form_data.getlist(f'filter_by_{task_type}')
    
    custom_groups = {}
    for key, value in form_data.items():
        if key.startswith('group_name_'):
            group_id = key.split('_')[-1]
            custom_groups[group_id] = {
                'name': value,
                'task_ids': [int(id.strip()) for id in form_data.get(f'group_task_ids_{group_id}', '').split(',') if id.strip()],
                'filter_by': form_data.getlist(f'group_filter_by_{group_id}')
            }
    config['custom_groups'] = custom_groups

@app.errorhandler(Exception)
def handle_error(e):
    app.logger.error(f'An error occurred: {str(e)}')
    return render_template('error.html', error_message=str(e)), 500

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run(debug=True)