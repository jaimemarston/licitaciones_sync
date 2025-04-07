import requests
import psycopg2
from datetime import datetime, timedelta
import pytz

# Configuración de la API SEACE
API_URL = "https://contratacionesabiertas.osce.gob.pe/api/v1/records"
PARAMS = {
    'order': 'desc',
    'sourceId': 'seace_v3',
    'limit': 100  # Ajusta según tus necesidades
}

# Configuración de la base de datos PostgreSQL

DB_CONFIG = {
    'dbname': 'licidata',
    'user': 'odoo',
    'password': 'odoo',
    'host': '212.227.28.47',
    'port': 5432
}


# Función para conectarse a la base de datos
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Función para obtener los registros desde la API
def fetch_cronograma_data():
    try:
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
        return data.get('records', [])
    except requests.exceptions.RequestException as e:
        print(f"? Error al conectar con la API: {e}")
        return []

# Función para actualizar cronograma en la base de datos
def update_cronograma(records):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        for record in records:
            compiled = record.get('compiledRelease', {})
            tender = compiled.get('tender', {})
            
            licitacion_id = tender.get('id')
            if not licitacion_id:
                print(f"?? Licitación sin ID detectada y omitida: {tender}")
                continue
            
            # Procesar los periodos de la licitación
            periods = {
                "tender": tender.get('tenderPeriod', {}),
                "enquiry": tender.get('enquiryPeriod', {})
            }

            for period_type, period_data in periods.items():
                start_date = period_data.get('startDate')
                end_date = period_data.get('endDate')

                # Verificar si start_date existe antes de intentar formatearlo
                if start_date:
                    fecha_original = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S%z")
                    fecha_corregida = fecha_original + timedelta(hours=5)
                    start_date = fecha_corregida.replace(tzinfo=None)

                if end_date:
                    fecha_original_end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S%z")
                    fecha_corregida_end = fecha_original_end + timedelta(hours=5)
                    end_date = fecha_corregida_end.replace(tzinfo=None)

                # Verificar si ya existe el cronograma
                cursor.execute("""
                    SELECT id FROM licitaciones_cronograma 
                    WHERE licitacion_id = %s AND title = %s
                """, (licitacion_id, period_type))
                result = cursor.fetchone()

                if result:
                    # Actualizar el cronograma existente
                    cursor.execute("""
                        UPDATE licitaciones_cronograma
                        SET fecha_inicio = %s, fecha_fin = %s
                        WHERE licitacion_id = %s AND title = %s
                    """, (start_date, end_date, licitacion_id, period_type))
                    print(f"?? Cronograma actualizado para licitación {licitacion_id} ({period_type})")
                else:
                    # Insertar un nuevo cronograma
                    cursor.execute("""
                        INSERT INTO licitaciones_cronograma (licitacion_id, title, fecha_inicio, fecha_fin)
                        VALUES (%s, %s, %s, %s)
                    """, (licitacion_id, period_type, start_date, end_date))
                    print(f"? Nuevo cronograma insertado para licitación {licitacion_id} ({period_type})")

        connection.commit()
        print("? Actualización de cronograma completada.")

    except (Exception, psycopg2.DatabaseError) as e:
        connection.rollback()
        print(f"? Error al actualizar cronograma: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()

# Ejecución principal
if __name__ == "__main__":
    records = fetch_cronograma_data()
    if records:
        update_cronograma(records)
