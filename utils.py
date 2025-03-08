# utils.py

def get_or_create_comprador(cursor, buyer):
    """Busca o crea un comprador en la tabla licitaciones_buyer."""
    if not buyer:
        return None

    cursor.execute("SELECT id FROM licitaciones_buyer WHERE name = %s", (buyer.get('name'),))
    result = cursor.fetchone()
    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO licitaciones_buyer (name, rol, direccion, localidad, region, pais)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        buyer.get('name'),
        ','.join(buyer.get('roles', [])),
        buyer.get('address', {}).get('streetAddress'),
        buyer.get('address', {}).get('locality'),
        buyer.get('address', {}).get('region'),
        buyer.get('address', {}).get('countryName')
    ))

    return cursor.fetchone()[0]

def get_or_create_proveedor(cursor, tenderer):
    """Busca o crea un proveedor en la tabla licitaciones_proveedor."""
    if not tenderer:
        return None

    ruc = tenderer.get('id')
    nombre = tenderer.get('name')

    cursor.execute("SELECT id FROM licitaciones_proveedor WHERE ruc = %s", (ruc,))
    result = cursor.fetchone()

    if result:
        return result[0]

    cursor.execute("""
        INSERT INTO licitaciones_proveedor (name, ruc, direccion, localidad, region, pais, correo, telefono, categoria)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        nombre,
        ruc,
        tenderer.get('address', {}).get('streetAddress'),
        tenderer.get('address', {}).get('locality'),
        tenderer.get('address', {}).get('region'),
        tenderer.get('address', {}).get('countryName'),
        tenderer.get('contactPoint', {}).get('email'),
        tenderer.get('contactPoint', {}).get('telephone'),
        'servicio'
    ))

    return cursor.fetchone()[0]

def translate_category(categoria):
    """Convierte categorías de inglés a español."""
    CATEGORIA_MAP = {
        'goods': 'bienes',
        'works': 'obras',
        'services': 'servicios'
    }
    return CATEGORIA_MAP.get(categoria, categoria)


def insert_or_update_favorites(cursor):
    """ Vincula licitaciones con favoritos basado en `claves` y `objeto_contratacion`."""
    
    # 1️⃣ Obtener relaciones que deberían existir
    cursor.execute("""
        WITH matching_licitaciones AS (
            SELECT f.id AS favorites_id, l.id AS licitacion_id
            FROM licitaciones_favorites f
            JOIN licitaciones_licitacion l ON l.objeto_contratacion ILIKE '%' || f.claves || '%'
        )
        SELECT favorites_id, licitacion_id FROM matching_licitaciones;
    """)
    
    relaciones = cursor.fetchall()  # Lista de tuplas (favorites_id, licitacion_id)

    # 2️⃣ Insertar solo si no existe
    for favorites_id, licitacion_id in relaciones:
        cursor.execute("""
            SELECT 1 FROM licitaciones_favorites_licitaciones_licitacion_rel
            WHERE licitaciones_favorites_id = %s AND licitaciones_licitacion_id = %s
        """, (favorites_id, licitacion_id))

        if cursor.fetchone() is None:  # Si no existe, insertamos
            cursor.execute("""
                INSERT INTO licitaciones_favorites_licitaciones_licitacion_rel (licitaciones_favorites_id, licitaciones_licitacion_id)
                VALUES (%s, %s)
            """, (favorites_id, licitacion_id))
    
            print("✅ Relaciones actualizadas en `licitaciones_favorites_licitaciones_licitacion_rel`.")


def update_bidder_winner(cursor):
    """Actualiza el campo bidder_winner en la tabla licitaciones_licitacion con el ID del postor ganador."""
    
    cursor.execute("""
        WITH ganador AS (
            SELECT lp.licitacion_id, lp.supplier_id
            FROM licitaciones_postores lp
            WHERE lp.ganador = TRUE
        )
        UPDATE licitaciones_licitacion ll
        SET bidder_winner = ganador.supplier_id
        FROM ganador
        WHERE ll.id = ganador.licitacion_id;
    """)

    print("✅ Se ha actualizado el campo `bidder_winner` en `licitaciones_licitacion` con el postor ganador.")
