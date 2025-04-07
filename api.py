import requests
from config import API_URL, PARAMS

def fetch_data_from_api(start_date=None, end_date=None):
    """
    Obtiene todos los registros de la API usando paginación y permite filtrar por rango de fechas.

    Parámetros:
        start_date (str, opcional): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date (str, opcional): Fecha de fin en formato 'YYYY-MM-DD'.

    Retorna:
        list: Lista de registros obtenidos.
    """
    all_records = []
    page = 1

    while True:
        # Construcción de la URL con los parámetros correctos
        params = {
            'page': page,
            'order': 'desc',
            'sourceId': 'seace_v3',
        }

        # Agregar las fechas si están disponibles
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date

        try:
            # Realizar la solicitud a la API
            response = requests.get(API_URL, params=params)
            print(f"🌐 URL solicitada: {response.url}")
            response.raise_for_status()

            # Procesar la respuesta
            data = response.json()
            records = data.get('records', [])
            
            # Si no hay registros, terminar el bucle
            if not records:
                print(f"⚠️ No se encontraron registros en la página {page}.")
                break

            all_records.extend(records)
            print(f"📄 Página {page} - Registros obtenidos: {len(records)}")
            page += 1  # Avanzar a la siguiente página

        except requests.exceptions.RequestException as e:
            print(f"❌ Error al conectar con la API en página {page}: {e}")
            break

    print(f"🔍 Total de registros obtenidos: {len(all_records)}")
    return all_records

# Pruebas
if __name__ == "__main__":
    print("\n🔹 Consulta por rango de fechas:")
    registros_fecha = fetch_data_from_api(start_date="2025-04-01", end_date="2025-04-03")
