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
        i = 0  # Inicializar contador antes del bucle
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

           # 📌 Insertar la licitación solo si no existe
            cursor.execute("""
                INSERT INTO licitaciones_licitacion (
                    id_licitacion, titulo, fecha_publicacion, objeto_contratacion,
                    monto_total, moneda, buyer_id, metodo_adquisicion, categoria_adquisicion
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_licitacion) DO UPDATE 
                SET titulo = EXCLUDED.titulo, 
                    fecha_publicacion = EXCLUDED.fecha_publicacion, 
                    objeto_contratacion = EXCLUDED.objeto_contratacion, 
                    monto_total = EXCLUDED.monto_total, 
                    moneda = EXCLUDED.moneda, 
                    buyer_id = EXCLUDED.buyer_id, 
                    metodo_adquisicion = EXCLUDED.metodo_adquisicion, 
                    categoria_adquisicion = EXCLUDED.categoria_adquisicion
                RETURNING id;
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
            licitacion_id = cursor.fetchone()  # `RETURNING id` asegurará que `licitacion_id` tenga un valor

            if licitacion_id:
                licitacion_id = licitacion_id[0]  # Extraer el ID correctamente
                
            #insert_or_update_favorites(cursor)
          
            # Diccionario de traducción de términos
            translation = {
                "tender": "Convocatoria",
                "enquiry": "Formulación de consultas"
            }

            # Procesar los periodos de la licitación
            periods = {
                translation["tender"]: tender.get('tenderPeriod', {}),
                translation["enquiry"]: tender.get('enquiryPeriod', {})
            }

            # # Procesar los periodos de la licitación
            # periods = {
            #     "tender": tender.get('tenderPeriod', {}),
            #     "enquiry": tender.get('enquiryPeriod', {})
            # }

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

                title_period = period_type.strip() if period_type else f"Sin título {licitacion_id}"
                current_timestamp = datetime.now()

                if start_date and end_date and title_period:  # Verificar que los valores no sean nulos o vacíos
                    try:
                        # Insertar o actualizar el cronograma usando ON CONFLICT
                        cursor.execute("""
                            INSERT INTO licitaciones_cronograma (licitacion_id, title, fecha_inicio, fecha_fin, name, write_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (licitacion_id, title) DO UPDATE 
                            SET fecha_inicio = EXCLUDED.fecha_inicio,
                                fecha_fin = EXCLUDED.fecha_fin,
                                write_date = EXCLUDED.write_date;
                        """, (licitacion_id, title_period, start_date, end_date, licitacion_id, current_timestamp))

                        #print(f"✅ Registro insertado o actualizado: {title_period} (ID: {licitacion_id})")

                    except Exception as e:
                        print(f"❌ Error al insertar o actualizar en `licitaciones_cronograma` -> licitacion_id: {licitacion_id}, title: {title_period}")
                        print(f"🛑 Error detallado: {e}")
                                
                #print(f"📅 Periodo '{period_type}' insertado para la licitación {licitacion_id}")

            # Procesar proveedores y postores
            tenderers = tender.get('tenderers', [])
            suppliers = {s['id'] for award in compiled.get('awards', []) for s in award.get('suppliers', [])}
            bidder_winner_id = None  # Variable para almacenar el ID del ganador

            for tenderer in tenderers:
                proveedor_id = get_or_create_proveedor(cursor, tenderer)
                if not proveedor_id:
                    print(f"⚠️ No se pudo obtener ID para el proveedor: {tenderer}")
                    continue

                ruc = tenderer.get('id')
                nombre = tenderer.get('name')
                ganador = ruc in suppliers
                ganador_estado = 'ganador' if ganador else None
                postores_id = proveedor_id if proveedor_id is not None else -1

  
                cursor.execute("""
                    INSERT INTO licitaciones_postores (
                        licitacion_id, supplier_id, postores_id, nombre, ganador, ganador_estado, titulo, fecha_publicacion
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (licitacion_id, supplier_id) DO UPDATE 
                    SET nombre = EXCLUDED.nombre,
                        ganador = EXCLUDED.ganador,
                        ganador_estado = EXCLUDED.ganador_estado,
                        titulo = EXCLUDED.titulo,
                        fecha_publicacion = EXCLUDED.fecha_publicacion;
                """, (
                    licitacion_id, proveedor_id, postores_id, nombre, ganador, ganador_estado, licitacion_description, licitacion_fecha_publicacion
                ))

            # Insertar documentos relacionados
            documents = tender.get('documents', [])
            for doc in documents:
                title = doc.get('title') if doc.get('title') else f"Sin título {licitacion_id}"  # Agrega el ID para evitar duplicados
                url = doc.get('url') if doc.get('url') else "Sin url"  # Asigna "URL" si no tiene

                try:
                    cursor.execute("""
                        INSERT INTO licitaciones_cronograma (
                            licitacion_id, url, fecha_inicio, tipo_documento, title, name, fecha_fin
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (licitacion_id, title) DO UPDATE 
                        SET fecha_inicio = EXCLUDED.fecha_inicio,
                            tipo_documento = EXCLUDED.tipo_documento,
                            title = EXCLUDED.title,
                            name = EXCLUDED.name,
                            fecha_fin = EXCLUDED.fecha_fin;
                    """, (
                        licitacion_id, url, doc.get('datePublished'), doc.get('documentType'), title, licitacion_id, doc.get('datePublished')
                    ))

                except Exception as e:
                    print(f"❌ Error al insertar en `licitaciones_cronograma` -> licitacion_id: {licitacion_id}, title: {title}, url: {url}")
                    print(f"🛑 Error detallado: {e}")


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
                try:
                    cursor.execute("""
                        INSERT INTO licitaciones_item (
                            id_item, posicion, descripcion, estado_detalle, estado, 
                            clasificacion_id, clasificacion_descripcion, clasificacion_esquema, 
                            cantidad, unidad_id, unidad_nombre, valor_total, moneda, licitacion_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id_item, licitacion_id) DO UPDATE 
                        SET posicion = EXCLUDED.posicion,
                            descripcion = EXCLUDED.descripcion,
                            estado_detalle = EXCLUDED.estado_detalle,
                            estado = EXCLUDED.estado,
                            clasificacion_id = EXCLUDED.clasificacion_id,
                            clasificacion_descripcion = EXCLUDED.clasificacion_descripcion,
                            clasificacion_esquema = EXCLUDED.clasificacion_esquema,
                            cantidad = EXCLUDED.cantidad,
                            unidad_id = EXCLUDED.unidad_id,
                            unidad_nombre = EXCLUDED.unidad_nombre,
                            valor_total = EXCLUDED.valor_total,
                            moneda = EXCLUDED.moneda;
                    """, (
                        id_item, posicion, descripcion, estado_detalle, estado,
                        clasificacion_id, clasificacion_descripcion, clasificacion_esquema,
                        cantidad, unidad_id, unidad_nombre, valor_total, moneda, licitacion_id
                    ))
                    # Actualizar el estado de la licitación si el ítem es 'active'
                    if estado == 'active':
                        cursor.execute("""
                            UPDATE licitaciones_licitacion
                            SET estado_item = 'active'
                            WHERE id = %s;
                        """, (licitacion_id,))


                except Exception as e:
                    connection.rollback()  # Revertir cambios en caso de error
                    print(f"❌ Error al insertar ítem -> id_item: {id_item}, licitacion_id: {licitacion_id}")
                    print(f"🛑 Error detallado: {e}")
          
            #connection.commit()  # Hacer commit cada 50 registros
            # 📌 3️⃣ Despues de insertar/actualizar los postores, actualizar el ganador en la licitación
            
            #update_bidder_winner(cursor)  # Asignar postor ganador en la licitación
            #connection.commit()  # Hacer commit cada 50 registros
            print(f"✅ Licitación con ID: {licitacion_id}")
            i += 1  # Incrementar contador

            if i % 100 == 0:
                connection.commit()  # Hacer commit cada 100 registros
                print("🟢 Commit ejecutado después de 100 ítems.")

        # Ejecutar el último commit si quedaron registros sin guardar
        if i % 100 != 0:
            connection.commit()
            print("🟢 Commit final para ítems.")

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
