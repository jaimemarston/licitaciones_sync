from api import fetch_data_from_api
from process import insert_data_into_db
from datetime import datetime, timedelta
from utils import create_indexes  # Importar la función desde utils

def fetch_data_by_date_range(start_date, end_date):
    """
    Obtiene datos de la API en función de un rango de fechas considerando días específicos.

    Parámetros:
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'.
    """

    # Convertir las fechas de inicio y fin a objetos datetime
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    current_date = start_date

    while current_date <= end_date:
        # Formatear la fecha actual para el mensaje y la API
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"🔍 Procesando datos para el día: {date_str}")

        # Obtener datos de la API filtrados por la fecha específica
        records = fetch_data_from_api(
            start_date=date_str,
            end_date=date_str
        )

        if records:
            print(f"✅ {len(records)} registros obtenidos para el día: {date_str}. Insertando en la BD...")

            # Asegurar que los índices existen antes de insertar datos
            create_indexes()

            # Insertar los datos en la base de datos
            insert_data_into_db(records)
        else:
            print(f"⚠️ No se encontraron registros para el día: {date_str}")

        # Incrementar al siguiente día
        current_date += timedelta(days=1)

if __name__ == "__main__":
    # Definir el rango de fechas deseado
    start_date = "2025-03-01"  # Fecha de inicio
    end_date = "2025-03-05"    # Fecha de fin

    # Llamar a la función con el rango de fechas
    fetch_data_by_date_range(start_date, end_date)
