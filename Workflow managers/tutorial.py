import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3
from prefect import flow, task

# Extracción: Obtiene los datos de quejas desde la API del CFPB.
@task
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size': 10})
    response_json = json.loads(r.text)
    return response_json['hits']['hits']

# Transformación: Procesa el JSON para extraer la información relevante.
@task
def parse_complaint_data(raw):
    complaints = []
    Complaint = namedtuple('Complaint', ['date_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source', {})
        this_complaint = Complaint(
            date_received=source.get('date_received'),  # Corrección de typo ('date_recieved' → 'date_received')
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints

# Carga: Inserta los datos transformados en una base de datos SQLite.
@task
def store_complaints(parsed):
    create_script = '''
    CREATE TABLE IF NOT EXISTS complaint (
        date_received TEXT,
        state TEXT,
        product TEXT,
        company TEXT,
        complaint_what_happened TEXT
    );
    '''
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            conn.commit()

# Flujo principal usando la nueva sintaxis de Prefect 2.x
@flow
def etl_flow():
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    store_complaints(parsed)

# Ejecución del flujo
if __name__ == "__main__":
    etl_flow()
