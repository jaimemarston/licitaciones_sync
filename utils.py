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
