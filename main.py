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
    check_if_audience_exists, insert_audience, check_if_permission_exists, insert_permission, update_permission, \
    check_if_role_exists, insert_roles, update_roles

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
                    type = row.get('Type')
                    if type == "Tools" :
                        type_to_insert = "tool"
                    elif type == "Training Aid" :
                        type_to_insert = "training_aid"
                    else :
                        type_to_insert = type

                    if not name:
                        print(f"Skipping row {index} due to missing 'Name' value.")
                        continue

                    # commented due to removing the update logic
                    # resource_id = check_if_resource_exists(connection, name)

                    # if resource_id is None:

                    insert_resource(connection, type_to_insert , part_no, name)

                    # else:
                    #     update_resource(connection, part_no, name)

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

@app.route('/permission', methods=['POST'])
def add_permissions():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            insert_count = 0
            update_Count = 0
            if connection:
                for index, row in df.iterrows():
                    request_type = row.get('request_type')
                    endpoint = row.get('endpoint')
                    http_method = row.get('http_method')

                    if not check_if_permission_exists(connection, request_type,endpoint,http_method):
                        insert_result = insert_permission(connection, request_type, endpoint, http_method)
                        if insert_result is True :
                            insert_count +=1
                    else:
                        update_result = update_permission(connection, request_type, endpoint, http_method)
                        if update_result is True :
                            update_Count += 1

                connection.close()
                return jsonify({
                    'message': 'File processed successfully' ,
                    'data_inserted' : insert_count,
                    'data_updated' : update_Count
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400

@app.route('/role', methods=['POST'])
def add_roles():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(file)
            connection = connect_to_db()
            insert_count = 0
            update_Count = 0
            if connection:
                for index, row in df.iterrows():
                    roles = row.get('role')
                    if not check_if_role_exists(connection, roles):
                        insert_result = insert_roles(connection, roles)
                        if insert_result is True :
                            insert_count +=1
                    else:
                        update_result = update_roles(connection, roles)
                        if update_result is True :
                            update_Count += 1

                connection.close()
                return jsonify({
                    'message': 'File processed successfully' ,
                    'data_inserted' : insert_count,
                    'data_updated' : update_Count
                }), 200
            else:
                return jsonify({'error': 'Failed to connect to database'}), 500
        except Exception as e:
            print("An error occurred:", e)
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Please upload an Excel file (.csv).'}), 400


@app.route('/map_roles_permissions', methods=['POST'])
def map_roles_permissions():
    try:
        connection = connect_to_db()
        if not connection:
            return jsonify({'error': 'Failed to connect to database'}), 500

        cursor = connection.cursor()
        cursor.execute("SELECT id, role_name FROM rbac_master")
        roles = cursor.fetchall()

        cursor.execute("SELECT id, request_type, endpoint, http_method FROM permission")
        permissions = cursor.fetchall()

        insert_count = 0
        update_count = 0

        for role in roles:
            for permission in permissions:
                role_id = role[0]
                permission_id = permission[0]
                cursor.execute("""
                    SELECT id FROM role_permission 
                    WHERE role_id = %s AND permission_id = %s
                """, (role_id, permission_id))
                result = cursor.fetchone()

                if result:
                    cursor.execute("""
                        UPDATE role_permission 
                        SET updated_at = now() 
                        WHERE id = %s
                    """, (result[0],))
                    update_count += 1
                else:
                    cursor.execute("""
                        INSERT INTO role_permission (role_id, permission_id, created_at, updated_at)
                        VALUES (%s, %s, now(), now())
                    """, (role_id, permission_id))
                    insert_count += 1

        connection.commit()
        connection.close()

        return jsonify({
            'message': 'Roles and permissions have been mapped successfully.',
            'data_inserted': insert_count,
            'data_updated': update_count
        }), 200

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True,port=5006)
