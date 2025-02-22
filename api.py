import requests
from config import API_URL, PARAMS

def fetch_data_from_api():
    """Obtiene todos los registros de la API usando paginación."""
    all_records = []
    page = 1

    while True:
        params = PARAMS.copy()
        params['page'] = page  # Agregar número de página dinámicamente

        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            records = data.get('records', [])
            if not records:
                break  # Si no hay más registros, salir del bucle

            all_records.extend(records)
            print(f"📄 Página {page} - Registros obtenidos: {len(records)}")

            page += 1  # Pasar a la siguiente página

        except requests.exceptions.RequestException as e:
            print(f"❌ Error al conectar con la API en página {page}: {e}")
            break

    print(f"🔍 Total de registros obtenidos: {len(all_records)}")
    return all_records

# Prueba la función
if __name__ == "__main__":
    registros = fetch_data_from_api()
