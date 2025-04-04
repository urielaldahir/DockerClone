import requests
import sqlite3
from contextlib import closing
from prefect import flow, task

# Parte 1: Extracción
@task
def extract_posts():
    url = "https://jsonplaceholder.cypress.io/posts"
    response = requests.get(url)
    return response.json()  # Se asume que la respuesta es JSON con una lista de posts

# Parte 2: Transformación
@task
def transform_posts(posts):
    # Convertimos cada post en una tupla (id, userId, title, body)
    return [(post.get("id"), post.get("userId"), post.get("title"), post.get("body")) for post in posts]

# Parte 3: Carga
@task
def load_posts(transformed_posts):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY,
        userId INTEGER,
        title TEXT,
        body TEXT
    );
    '''
    insert_query = "INSERT OR REPLACE INTO posts (id, userId, title, body) VALUES (?, ?, ?, ?)"
    
    with closing(sqlite3.connect("jsonplaceholder.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute(create_table_query)
            cursor.executemany(insert_query, transformed_posts)
            conn.commit()

# Flujo principal
@flow
def etl_jsonplaceholder():
    posts = extract_posts()
    transformed = transform_posts(posts)
    load_posts(transformed)

# Ejecución del flujo
if __name__ == "__main__":
    etl_jsonplaceholder()