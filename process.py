# process.py
from database import get_db_connection, clear_tables
from utils import get_or_create_comprador, get_or_create_proveedor, translate_category, insert_or_update_favorites, update_bidder_winner
import psycopg2
from datetime import datetime, timedelta
import pytz
import time

# Definir la zona horaria de Perú
peru_tz = pytz.timezone("America/Lima")
fmt = "%Y-%m-%dT%H:%M:%S%z"

def insert_data_into_db(records):
    """Inserta los datos en la base de datos PostgreSQL."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
      
        # Limpiar tablas antes de insertar nuevos datos
        #clear_tables(cursor)

        for record in records:
            compiled = record.get('compiledRelease', {})
            tender = compiled.get('tender', {})
            buyer = compiled.get('buyer', {})
            licitacion_description = tender.get('description'),
            licitacion_fecha_publicacion = tender.get('datePublished'),

            if not tender.get('id'):
                print(f"⚠️ Licitación sin ID detectada y omitida: {tender}")
                continue

            comprador_id = get_or_create_comprador(cursor, buyer)
            categoria_es = translate_category(tender.get('mainProcurementCategory', 'desconocido'))


            # 📌 Verificar si la licitación ya existe
            cursor.execute("SELECT id FROM licitaciones_licitacion WHERE id_licitacion = %s", (tender.get('id'),))
            licitacion_id = cursor.fetchone()

            if not licitacion_id:
                cursor.execute("""
                    INSERT INTO licitaciones_licitacion (
                        id_licitacion, titulo, fecha_publicacion, objeto_contratacion,
                        monto_total, moneda, buyer_id, metodo_adquisicion, categoria_adquisicion
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    tender.get('id'),
                    tender.get('title'),
                    tender.get('datePublished'),
                    tender.get('description'),
                    tender.get('value', {}).get('amount'),
                    tender.get('value', {}).get('currency'),
                    comprador_id,
                    tender.get('procurementMethodDetails'),
                    categoria_es
                ))
                licitacion_id = cursor.fetchone()[0]
            if 1==2:
                print(f"✅ Licitación insertada con ID: {licitacion_id}")
                # Insertar relación en la tabla pivote licitaciones_buyer_pivot
                cursor.execute("""
                    INSERT INTO licitaciones_buyer_pivot (tender_id, buyer_id)
                    VALUES (%s, %s)
                """, (licitacion_id, comprador_id))

            #print(f"✅ Relación insertada en `licitaciones_buyer_pivot`: Licitación {licitacion_id} ↔ Comprador {comprador_id}")
                    # 🟢 📌 Ahora llamamos a `insert_or_update_favorites()` después de insertar todas las licitaciones
            insert_or_update_favorites(cursor)
            update_bidder_winner(cursor)  # Asignar postor ganador en la licitación
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
                    start_date = fecha_corregida.replace(tzinfo=None)  # Eliminar la zona horaria para evitar conversiones
                
                # Si end_date no existe, usar start_date corregido
                if not end_date:
                    end_date = start_date
                else:
                    fecha_original_end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S%z")
                    fecha_corregida_end = fecha_original_end + timedelta(hours=5)
                    end_date = fecha_corregida_end.replace(tzinfo=None)  # Eliminar la zona horaria
                    

                duration_days = period_data.get('durationInDays')

                if start_date and end_date:
                    

                    cursor.execute("""
                        WITH upsert AS (
                            UPDATE licitaciones_cronograma
                            SET fecha_inicio = %s, fecha_fin = %s
                            WHERE licitacion_id = %s AND title = %s
                            RETURNING id
                        )
                        INSERT INTO licitaciones_cronograma (licitacion_id, title, fecha_inicio, fecha_fin, name)
                        SELECT %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (SELECT 1 FROM upsert);
                    """, (start_date, end_date, licitacion_id, period_type, 
                        licitacion_id, period_type, start_date, end_date, licitacion_id))
                    
               #print(f"📅 Periodo '{period_type}' insertado para la licitación {licitacion_id}")

            # Procesar proveedores y postores
            tenderers = tender.get('tenderers', [])
            suppliers = {s['id'] for award in compiled.get('awards', []) for s in award.get('suppliers', [])}

            for tenderer in tenderers:
                proveedor_id = get_or_create_proveedor(cursor, tenderer)
                if not proveedor_id:
                    print(f"⚠️ No se pudo obtener ID para el proveedor: {tenderer}")
                    continue

                ruc = tenderer.get('id')
                nombre = tenderer.get('name')
                ganador = ruc in suppliers
                ganador_estado = 'ganador' if ganador else None
                cursor.execute("""
                    WITH upsert AS (
                        UPDATE licitaciones_postores
                        SET nombre = %s, ganador = %s, ganador_estado = %s, titulo = %s, fecha_publicacion = %s
                        WHERE licitacion_id = %s AND supplier_id = %s
                        RETURNING id
                    )
                    INSERT INTO licitaciones_postores (licitacion_id, supplier_id, nombre, ganador, ganador_estado, titulo, fecha_publicacion)
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM upsert);
                """, (
                    # Parámetros para UPDATE
                    nombre, ganador, ganador_estado, licitacion_description, licitacion_fecha_publicacion, licitacion_id, proveedor_id,

                    # Parámetros para INSERT
                    licitacion_id, proveedor_id, nombre, ganador, ganador_estado, licitacion_description, licitacion_fecha_publicacion
                ))
                
            # Insertar documentos relacionados
            documents = tender.get('documents', [])
            for doc in documents:
                title = doc.get('title') if doc.get('title') else "Sin título"  # Asigna "Sin título" si no tiene
                url = doc.get('url') if doc.get('url') else "Sin url"  # Asigna "URL" si no tiene
            
                cursor.execute("""
                    WITH upsert AS (
                        UPDATE licitaciones_cronograma
                        SET fecha_inicio = %s, tipo_documento = %s, title = %s, name = %s, fecha_fin = %s
                        WHERE licitacion_id = %s AND url = %s
                        RETURNING id
                    )
                    INSERT INTO licitaciones_cronograma (
                        licitacion_id, url, fecha_inicio, tipo_documento, title, name, fecha_fin
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM upsert);
                    """, (
                        doc.get('datePublished'), doc.get('documentType'), title, licitacion_id, doc.get('datePublished'),
                        licitacion_id, url,  # UPDATE WHERE
                        licitacion_id, url, doc.get('datePublished'), doc.get('documentType'), title, licitacion_id, doc.get('datePublished')  # INSERT
                    ))


            # Insertar ítems relacionados
            items = tender.get('items', [])
            for item in items:
                id_item = item.get('id')  # ID del ítem
                posicion = item.get('position')  # Posición del ítem
                descripcion = item.get('description')  # Descripción del ítem
                estado_detalle = item.get('statusDetails')  # Detalles del estado
                estado = item.get('status')  # Estado general
                clasificacion_id = item.get('classification', {}).get('id')  # ID de la clasificación
                clasificacion_descripcion = item.get('classification', {}).get('description')  # Descripción de la clasificación
                clasificacion_esquema = item.get('classification', {}).get('scheme')  # Esquema de la clasificación
                cantidad = item.get('quantity')  # Cantidad del ítem
                unidad_id = item.get('unit', {}).get('id')  # ID de la unidad de medida
                unidad_nombre = item.get('unit', {}).get('name')  # Nombre de la unidad de medida
                valor_total = item.get('totalValue', {}).get('amount')  # Valor total del ítem
                moneda = item.get('totalValue', {}).get('currency')  # Moneda del ítem

                # Validar campos obligatorios antes de insertar
                if not id_item or not descripcion:
                    print(f"Ítem ignorado por datos incompletos: {item}")
                    continue

                # Insertar ítem en la base de datos
                cursor.execute("""
                        WITH upsert AS (
                            UPDATE licitaciones_item
                            SET posicion = %s, descripcion = %s, estado_detalle = %s, estado = %s,
                                clasificacion_id = %s, clasificacion_descripcion = %s, clasificacion_esquema = %s,
                                cantidad = %s, unidad_id = %s, unidad_nombre = %s, valor_total = %s, moneda = %s
                            WHERE licitacion_id = %s AND id_item = %s
                            RETURNING id
                        )
                        INSERT INTO licitaciones_item (
                            id_item, posicion, descripcion, estado_detalle, estado, 
                            clasificacion_id, clasificacion_descripcion, clasificacion_esquema, 
                            cantidad, unidad_id, unidad_nombre, valor_total, moneda, licitacion_id
                        )
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (SELECT 1 FROM upsert);
                    """, (
                        posicion, descripcion, estado_detalle, estado,
                        clasificacion_id, clasificacion_descripcion, clasificacion_esquema,
                        cantidad, unidad_id, unidad_nombre, valor_total, moneda,
                        licitacion_id, id_item,  # UPDATE WHERE
                        id_item, posicion, descripcion, estado_detalle, estado,
                        clasificacion_id, clasificacion_descripcion, clasificacion_esquema,
                        cantidad, unidad_id, unidad_nombre, valor_total, moneda, licitacion_id  # INSERT
                    ))

        connection.commit()
        print("✅ Todos los datos han sido insertados correctamente.")

    except psycopg2.DatabaseError as e:
        connection.rollback()  # Revertir cambios en caso de error
        
        # Obtener el código de error de PostgreSQL y el mensaje detallado
        error_code = e.pgcode if hasattr(e, 'pgcode') else "Sin código"
        error_message = e.pgerror if hasattr(e, 'pgerror') else str(e)

        print(f"❌ Error en la base de datos (Código: {error_code}): {error_message}")


    finally:
        if connection:
            cursor.close()
            connection.close()
