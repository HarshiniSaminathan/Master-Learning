from flask import Flask, request, jsonify
import pandas as pd
import psycopg2
from psycopg2 import Error
import openpyxl



def fetch_competency_id(connection, competency_name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.competencies WHERE competency_name = %s"
        cursor.execute(sql, (competency_name,))
        competency_id = cursor.fetchone()
        if competency_id:
            return competency_id[0]
        else:
            print(f"Competency '{competency_name}' not found in database.")
            return None
    except (Exception, Error) as error:
        print(f"Error fetching competency ID for '{competency_name}':", error)
        return None

def fetch_proficiency_id(connection, level_name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.proficiencies WHERE level_name = %s"
        cursor.execute(sql, (level_name,))
        proficiency_id = cursor.fetchone()
        if proficiency_id:
            return proficiency_id[0]
        else:
            print(f"Proficiency level '{level_name}' not found in database.")
            return None
    except (Exception, Error) as error:
        print(f"Error fetching proficiency ID for '{level_name}':", error)
        return None

def fetch_domain_id(connection, domain_name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.domains WHERE domain_name = %s"
        cursor.execute(sql, (domain_name,))
        domain_id = cursor.fetchone()
        if domain_id:
            return domain_id[0]
        else:
            print(f"Proficiency level '{domain_name}' not found in database.")
            return None
    except (Exception, Error) as error:
        print(f"Error fetching proficiency ID for '{domain_name}':", error)
        return None

def insert_master_learning_outcomes(connection, data):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO public.master_learning_outcomes (domain_id,competency_id, proficiency_level_id, outcome, created_at, updated_at) VALUES (%s,%s, %s, %s, now(), now())"
        cursor.execute(sql, data)
        connection.commit()
        print("Data inserted into master_learning_outcomes successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into master_learning_outcomes:", error)

def check_if_designation_exists(connection, name):
    try:
        cursor = connection.cursor()
        sql = "SELECT COUNT(*) FROM public.designation WHERE name = %s"
        cursor.execute(sql, (name,))
        count = cursor.fetchone()[0]
        return count > 0
    except (Exception, Error) as error:
        print(f"Error checking if designation '{name}' exists:", error)
        return False

def insert_designation(connection, name):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO public.designation (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (name,))
        connection.commit()
        print(f"Data for '{name}' inserted into designation table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into designation table:", error)

def check_if_resource_exists(connection, name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.resources WHERE name = %s"
        cursor.execute(sql, (name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if resource '{name}' exists:", error)
        return None

def insert_resource(connection, type , part_no, name):
    try:
        cursor = connection.cursor()
        sql = """
        INSERT INTO public.resources (type ,part_no, name, created_at, updated_at)
        VALUES (%s ,%s , %s, now(), now())
        """
        cursor.execute(sql, (type , part_no , name,))
        connection.commit()
        print(f"Data for '{name}' inserted into resources table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into resources table:", error)

def update_resource(connection, part_no, name):
    try:
        cursor = connection.cursor()
        sql = """
        UPDATE public.resources
        SET part_no = %s, created_at = now(), updated_at = now()
        WHERE name = %s
        """
        cursor.execute(sql, (part_no, name))
        connection.commit()
        print(f"Data for '{name}' updated in resources table successfully.")
    except (Exception, Error) as error:
        print("Error updating data in resources table:", error)

def check_if_region_exists(connection, name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.regions WHERE name = %s"
        cursor.execute(sql, (name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if region '{name}' exists:", error)
        return None

def insert_region(connection, name):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO public.regions (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (name,))
        connection.commit()
        print(f"Data for '{name}' inserted into region table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into region table:", error)


def check_if_business_exists(connection, name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM public.business_group WHERE name = %s"
        cursor.execute(sql, (name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if Business name '{name}' exists:", error)
        return None

def insert_business(connection, name):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO public.business_group (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (name,))
        connection.commit()
        print(f"Data for '{name}' inserted into business table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into business table:", error)

def check_if_audience_exists(connection, name):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM intended_audience WHERE name = %s"
        cursor.execute(sql, (name,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if audience name '{name}' exists:", error)
        return None

def check_if_permission_exists(connection, request_type,endpoint,http_method):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM permission WHERE request_type = %s AND endpoint = %s AND http_method = %s"
        cursor.execute(sql, (request_type,endpoint,http_method,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if  permission '{request_type} {endpoint} {http_method}' exists:", error)
        return None

def check_if_role_exists(connection, roles):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM rbac_master WHERE role_name = %s"
        cursor.execute(sql, (roles,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if rbac master '{roles}' exists:", error)
        return None

def check_if_sbu_exists(connection, sbu):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM sbu WHERE name = %s"
        cursor.execute(sql, (sbu,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if sbu '{sbu}' exists:", error)
        return None

def check_if_job_role_exists(connection, job_role):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM job_role WHERE name = %s"
        cursor.execute(sql, (job_role,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if job_role '{job_role}' exists:", error)
        return None

def check_if_industry_exists(connection, industry):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM industry WHERE name = %s"
        cursor.execute(sql, (industry,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if industry '{industry}' exists:", error)
        return None


def check_if_grade_exists(connection, grade):
    try:
        cursor = connection.cursor()
        sql = "SELECT id FROM grade WHERE name = %s"
        cursor.execute(sql, (grade,))
        result = cursor.fetchone()
        return result[0] if result else None
    except (Exception, Error) as error:
        print(f"Error checking if grade '{grade}' exists:", error)
        return None

def insert_audience(connection, name):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO  intended_audience (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (name,))
        connection.commit()
        print(f"Data for '{name}' inserted into audience table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into audience table:", error)

def insert_sbu(connection, sbu):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO  sbu (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (sbu,))
        connection.commit()
        print(f"Data for '{sbu}' inserted into sbu table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into sbu table:", error)

def insert_job_role(connection, job_role):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO  job_role (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (job_role,))
        connection.commit()
        print(f"Data for '{job_role}' inserted into job_role table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into job_role table:", error)

def insert_grade(connection, grade):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO grade (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (grade,))
        connection.commit()
        print(f"Data for '{grade}' inserted into grade table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into grade table:", error)

def insert_industry(connection, industry):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO industry (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (industry,))
        connection.commit()
        print(f"Data for '{industry}' inserted into industry table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into industry table:", error)

def insert_permission(connection, request_type, endpoint, http_method):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO permission (request_type, endpoint, http_method , created_at, updated_at) VALUES ( %s ,%s ,%s ,now(), now())"
        cursor.execute(sql, (request_type, endpoint, http_method,))
        connection.commit()
        print(f"Data for '{request_type} {endpoint} {http_method}' inserted into permission table successfully.")
        return True
    except (Exception, Error) as error:
        print("Error inserting data into permission table:", error)
        return False

def insert_roles(connection, roles):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO rbac_master (role_name, created_at, updated_at) VALUES ( %s ,now(), now())"
        cursor.execute(sql, (roles,))
        connection.commit()
        print(f"Data for '{roles}' inserted into role table successfully.")
        return True
    except (Exception, Error) as error:
        print("Error inserting data into role table:", error)
        return False

def update_permission(connection, request_type, endpoint, http_method):
    try:
        cursor = connection.cursor()
        sql = """
        UPDATE permission
        SET request_type = %s, endpoint = %s, http_method = %s, updated_at = now()
        WHERE request_type = %s AND endpoint = %s AND http_method = %s
        """
        cursor.execute(sql, (request_type, endpoint, http_method, request_type, endpoint, http_method))
        connection.commit()
        print(f"Data for '{request_type} {endpoint} {http_method}' updated in permission table successfully.")
        return True
    except (Exception, Error) as error:
        print("Error updating data in permission table:", error)
        return False

def update_roles(connection, roles):
    try:
        cursor = connection.cursor()
        sql = """
        UPDATE rbac_master
        SET updated_at = now()
        WHERE role_name = %s
        """
        cursor.execute(sql, (roles,))
        connection.commit()
        print(f"Data for '{roles} ' updated in role table successfully.")
        return True
    except (Exception, Error) as error:
        print("Error updating data in role table:", error)
        return False
