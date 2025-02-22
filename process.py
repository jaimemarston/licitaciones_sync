# process.py
from database import get_db_connection, clear_tables
from utils import get_or_create_comprador, get_or_create_proveedor, translate_category

def insert_data_into_db(records):
    """Inserta los datos en la base de datos PostgreSQL."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Limpiar tablas antes de insertar nuevos datos
        clear_tables(cursor)

        for record in records:
            compiled = record.get('compiledRelease', {})
            tender = compiled.get('tender', {})
            buyer = compiled.get('buyer', {})

            if not tender.get('id'):
                print(f"⚠️ Licitación sin ID detectada y omitida: {tender}")
                continue

            comprador_id = get_or_create_comprador(cursor, buyer)
            categoria_es = translate_category(tender.get('mainProcurementCategory', 'desconocido'))

            cursor.execute("""
                INSERT INTO licitaciones_licitacion (
                    id_licitacion, titulo, fecha_publicacion, objeto_contratacion,
                    monto_total, moneda, comprador_id, metodo_adquisicion, categoria_adquisicion
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                tender.get('id'),
                tender.get('title'),
                compiled.get('date'),
                tender.get('description'),
                tender.get('value', {}).get('amount'),
                tender.get('value', {}).get('currency'),
                comprador_id,
                tender.get('procurementMethodDetails'),
                categoria_es
            ))
            licitacion_id = cursor.fetchone()[0]
            print(f"✅ Licitación insertada con ID: {licitacion_id}")

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
                    INSERT INTO licitaciones_postores (
                        licitacion_id, postores_id, ruc, nombre, ganador, ganador_estado
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (licitacion_id, proveedor_id, ruc, nombre, ganador, ganador_estado))

            # Insertar documentos relacionados
            documents = tender.get('documents', [])
            for doc in documents:
                title = doc.get('title') if doc.get('title') else "Sin título"  # Asigna "Sin título" si no tiene
                url = doc.get('url') if doc.get('url') else "Sin url"  # Asigna "URL" si no tiene

                
                cursor.execute("""
                    INSERT INTO licitaciones_documento (
                        id_documento, url, fecha_publicacion, formato, tipo_documento, titulo, licitacion_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    doc.get('id'),
                    url,
                    doc.get('datePublished'),
                    doc.get('format'),
                    doc.get('documentType'),
                    title,
                    licitacion_id
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
                cursor.execute(
                    """
                    INSERT INTO licitaciones_item (
                        id_item, posicion, descripcion, estado_detalle, estado, 
                        clasificacion_id, clasificacion_descripcion, clasificacion_esquema, 
                        cantidad, unidad_id, unidad_nombre, valor_total, moneda, licitacion_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        id_item,                    # ID del ítem
                        posicion,                   # Posición
                        descripcion,                # Descripción
                        estado_detalle,             # Detalles del estado
                        estado,                     # Estado
                        clasificacion_id,           # ID de la clasificación
                        clasificacion_descripcion,  # Descripción de la clasificación
                        clasificacion_esquema,      # Esquema de la clasificación
                        cantidad,                   # Cantidad
                        unidad_id,                  # ID de la unidad de medida
                        unidad_nombre,              # Nombre de la unidad de medida
                        valor_total,                # Valor total
                        moneda,                     # Moneda
                        licitacion_id               # Relación con la licitación
                    )
                )


        connection.commit()
        print("✅ Todos los datos han sido insertados correctamente.")

    except Exception as e:
        connection.rollback()
        print("❌ Error al insertar datos en la base de datos:", e)

    finally:
        if connection:
            cursor.close()
            connection.close()
