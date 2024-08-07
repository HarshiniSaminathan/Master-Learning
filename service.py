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

def insert_resource(connection, part_no, name):
    try:
        cursor = connection.cursor()
        sql = """
        INSERT INTO public.resources (part_no, name, created_at, updated_at)
        VALUES (%s , %s, now(), now())
        """
        cursor.execute(sql, (part_no , name,))
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

def insert_audience(connection, name):
    try:
        cursor = connection.cursor()
        sql = "INSERT INTO  intended_audience (name, created_at, updated_at) VALUES (%s,now(), now())"
        cursor.execute(sql, (name,))
        connection.commit()
        print(f"Data for '{name}' inserted into audience table successfully.")
    except (Exception, Error) as error:
        print("Error inserting data into audience table:", error)