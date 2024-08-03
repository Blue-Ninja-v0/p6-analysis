# 📊 P6 Analysis Tool

## 🌟 Overview

P6 Analysis Tool is a Flask-based web application designed for analyzing and processing Primavera P6 XER files. This tool provides a user-friendly interface for uploading XER files, processing them through various stages of analysis, and generating comprehensive reports and visualizations. It is particularly useful for construction projects and other industries that rely heavily on project scheduling and cost management.

## 🚀 Features

- 📁 XER file upload and parsing
- 🔄 Multi-step processing pipeline:
  1. Data preprocessing and analysis
  2. Critical path analysis with backward and forward passes
  3. Comprehensive reporting
  4. Gantt chart generation
- 📈 Conversion of data to an easy-to-read Excel structure
- 💰 Cost profiling support for data that can't be loaded into P6
- 📊 Ability to model cost data in its native Excel form
- 📅 Create Excel activity-profiles (by month, bi-weekly, weekly, quarterly etc) using user-defined configurations to group specific activities and tasks

## 🛠️ Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/p6-analysis-tool.git
   cd p6-analysis-tool
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## ⚙️ Configuration

Customize the analysis settings by modifying the `config.yaml` file. This file allows you to define:

- 📋 Custom task groups
- 🔍 Task type filters
- 🎛️ Master settings (e.g., date range for analysis)

## 🚀 Usage

1. Start the Flask application:
   ```
   python app.py
   ```

2. Open a web browser and navigate to `http://localhost:5000`

3. Upload your XER file through the web interface

4. Monitor the processing progress and view the results

5. Configure and run the final analysis as needed

## 📁 Project Structure

- `app.py`: Main Flask application
- `orchestrator.py`: Coordinates the execution of processing scripts
- `config.yaml`: Configuration file for analysis settings
- `scripts/`: Directory containing individual processing scripts
- `templates/`: HTML templates for the web interface
- `static/`: Static files (CSS, JavaScript, etc.)
- `uploads/`: Directory for uploaded XER files and generated outputs

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
