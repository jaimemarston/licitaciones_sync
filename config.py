# config.py

DB_CONFIG = {
    'dbname': 'licidata',
    'user': 'odoo',
    'password': 'odoo',
    'host': '212.227.28.47',
    'port': 5432
}

#API_URL = "https://contratacionesabiertas.osce.gob.pe/api/v1/records"
API_URL = "https://contratacionesabiertas.osce.gob.pe/api/v1/records"

# Agregar paginación para traer más de 20 registros
PARAMS = {
    'order': 'desc',
    'sourceId': 'seace_v3',
    'dataSegmentationID': '2025-02',
    'tenderId':'',
    'page': 1,  # Paginación inicial
    'limit': 100  # Intenta aumentar este valor si la API lo permite
}
