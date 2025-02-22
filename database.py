# database.py
import psycopg2
from config import DB_CONFIG

def get_db_connection():
    """Establece conexión con la base de datos."""
    return psycopg2.connect(**DB_CONFIG)

def clear_tables(cursor):
    """Elimina datos existentes en las tablas relacionadas con licitaciones."""
    tables = [
        "licitaciones_postores",
        "licitaciones_proveedor",
        "licitaciones_documento",
        "licitaciones_item",
        "licitaciones_cronograma",
        "licitaciones_licitacion",
        "licitaciones_buyer"
    ]
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
        cursor.execute(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1")

    print("✅ Tablas limpiadas y secuencias reiniciadas correctamente.")
