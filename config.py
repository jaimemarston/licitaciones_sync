# config.py

DB_CONFIG = {
    'dbname': 'licidata',
    'user': 'odoo',
    'password': 'odoo',
    'host': '212.227.28.47',
    'port': 5432
}


API_URL = "https://contratacionesabiertas.osce.gob.pe/api/v1/records"


#'https://contratacionesabiertas.osce.gob.pe/api/v1/records?page=1&order=desc&sourceId=seace_v3&startDate=2024-01-01&endDate=2024-01-15' \


# Agregar paginación para traer más de 20 registros
""" PARAMS = {
    'order': 'desc',
    'sourceId': 'seace_v3',
    'dataSegmentationID': '2025-02',
    'tenderId':'992903',
    'page': 1,  # Paginación inicial
    'limit': 100  # Intenta aumentar este valor si la API lo permite
} """

PARAMS = {
    'order': 'desc',
    'sourceId': 'seace_v3',
    'ocid':'',
    'tenderId':'992903',
    'startDate':'2024-01-01',
    'endDate':'2024-01-03',
    'page': 1,  # Paginación inicial
    'limit': 100  # Intenta aumentar este valor si la API lo permite
}