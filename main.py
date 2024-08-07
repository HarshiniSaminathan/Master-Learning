import datetime
from datetime import datetime

from flask import Flask, request, jsonify
import pandas as pd
import psycopg2
from psycopg2 import Error
import openpyxl

from service import fetch_competency_id, fetch_proficiency_id, fetch_domain_id, insert_master_learning_outcomes, \
    check_if_designation_exists, insert_designation, check_if_resource_exists, update_resource, \
    insert_resource, check_if_region_exists, insert_region, check_if_business_exists, insert_business, \
    check_if_audience_exists, insert_audience

app = Flask(__name__)


import os
from dotenv import load_dotenv
load_dotenv()

db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def connect_to_db():
    try:
        connection = psycopg2.connect(**db_params)
        return connection
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None


@app.route('/upload_master_outcome', methods=['POST'])
def upload_file_outcome():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    competency_name = row['Competency Group']
                    proficiency_level = row['Proficiency Level']
                    learning_outcome = row['Learning outcome']

                    competency_id = fetch_competency_id(connection, competency_name)
                    if competency_id is None:
                        continue

                    proficiency_id = fetch_proficiency_id(connection, proficiency_level)
                    if proficiency_id is None:
                        continue

                    domain_id = fetch_domain_id(connection, "Service")
                    if domain_id is None:
                        continue

                    data = (domain_id, competency_id, proficiency_id, learning_outcome)
                    insert_master_learning_outcomes(connection, data)

                connection.close()
                return jsonify({'message': 'Data processed successfully'}), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500

        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format'}), 400


@app.route('/upload_master_designation', methods=['POST'])
def upload_file_designation():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    name = row.get('Designation')
                    if not name:
                        print(f"Skipping row {index} due to missing 'Designation' value.")
                        continue

                    if not check_if_designation_exists(connection, name):
                        insert_designation(connection, name)
                    else:
                        print(f"Designation '{name}' already exists. Skipping insertion.")

                connection.close()
                return jsonify({
                    'message': 'File processed successfully'
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400


@app.route('/upload_master_resources', methods=['POST'])
def upload_file_resources():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    part_no = row.get('Part No')
                    name = row.get('Name')

                    if not name:
                        print(f"Skipping row {index} due to missing 'Name' value.")
                        continue

                    resource_id = check_if_resource_exists(connection, name)

                    if resource_id is None:
                        insert_resource(connection, part_no, name)
                    else:
                        update_resource(connection, part_no, name)

                connection.close()
                return jsonify({
                    'message': 'File processed successfully'
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400

@app.route('/upload_master_region', methods=['POST'])
def upload_file_region():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    name = row.get('Name')
                    if not name:
                        print(f"Skipping row {index} due to missing 'Name' value.")
                        continue

                    if not check_if_region_exists(connection, name):
                        insert_region(connection, name)
                    else:
                        print(f"Region '{name}' already exists. Skipping insertion.")

                connection.close()
                return jsonify({
                    'message': 'File processed successfully'
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400

@app.route('/upload_business_group', methods=['POST'])
def upload_business_group():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    name = row.get('Name')
                    if not name:
                        print(f"Skipping row {index} due to missing 'Name' value.")
                        continue

                    if not check_if_business_exists(connection, name):
                        insert_business(connection, name)
                    else:
                        print(f"Business Name '{name}' already exists. Skipping insertion.")

                connection.close()
                return jsonify({
                    'message': 'File processed successfully'
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400

@app.route('/upload_audience', methods=['POST'])
def upload_audience():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            if connection:
                for index, row in df.iterrows():
                    name = row.get('Name')
                    if not name:
                        print(f"Skipping row {index} due to missing 'Name' value.")
                        continue

                    if not check_if_audience_exists(connection, name):
                        insert_audience(connection, name)
                    else:
                        print(f"Audience Name '{name}' already exists. Skipping insertion.")

                connection.close()
                return jsonify({
                    'message': 'File processed successfully'
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400

if __name__ == "__main__":
    app.run(debug=True,port=5050)
