import asyncio
import asyncpg
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dotenv import load_dotenv

load_dotenv()

# Variables de entorno
ERP_HOST = os.getenv("ERP_HOST")
ERP_PORT = os.getenv("ERP_PORT", "5432")
ERP_DB = os.getenv("ERP_DB")
ERP_USER = os.getenv("ERP_USER")
ERP_PASSWORD = os.getenv("ERP_PASSWORD")

DWH_HOST = os.getenv("DWH_HOST")
DWH_PORT = os.getenv("DWH_PORT", "5432")
DWH_DB = os.getenv("DWH_DB")
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")


async def get_erp_connection():
    """Conexión a la base de datos ERP (solo lectura)"""
    return await asyncpg.connect(
        host=ERP_HOST,
        port=ERP_PORT,
        database=ERP_DB,
        user=ERP_USER,
        password=ERP_PASSWORD
    )


async def get_dwh_connection():
    """Conexión a la base de datos DWH (lectura/escritura)"""
    return await asyncpg.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        database=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD
    )


async def populate_dim_tiempo(dwh_conn, start_date: datetime, end_date: datetime):
    """Puebla la dimensión de tiempo con un rango de fechas"""
    print("Poblando Dim_Tiempo...")
    
    dias_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    
    current_date = start_date
    batch = []
    
    while current_date <= end_date:
        dia_semana = current_date.weekday()
        es_fin_semana = dia_semana >= 5
        semana_anio = current_date.isocalendar()[1]
        trimestre = (current_date.month - 1) // 3 + 1
        semestre = 1 if current_date.month <= 6 else 2
        
        batch.append((
            current_date,
            current_date.year,
            current_date.month,
            current_date.day,
            trimestre,
            semestre,
            dia_semana,
            dias_semana[dia_semana],
            meses[current_date.month - 1],
            es_fin_semana,
            False,
            semana_anio
        ))
        
        if len(batch) >= 1000:
            await dwh_conn.executemany("""
                INSERT INTO Dim_Tiempo (
                    fecha, anio, mes, dia, trimestre, semestre, dia_semana,
                    nombre_dia_semana, nombre_mes, es_fin_semana, es_festivo, semana_anio
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (fecha) DO NOTHING
            """, batch)
            batch = []
        
        current_date += timedelta(days=1)
    
    if batch:
        await dwh_conn.executemany("""
            INSERT INTO Dim_Tiempo (
                fecha, anio, mes, dia, trimestre, semestre, dia_semana,
                nombre_dia_semana, nombre_mes, es_fin_semana, es_festivo, semana_anio
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (fecha) DO NOTHING
        """, batch)
    
    print(f"Dim_Tiempo poblada desde {start_date.date()} hasta {end_date.date()}")


async def extract_hoteles(erp_conn) -> List[Dict]:
    """Extrae hoteles del ERP"""
    print("Extrayendo Hoteles del ERP...")
    rows = await erp_conn.fetch("SELECT * FROM Hoteles")
    return [dict(row) for row in rows]


async def load_dim_hotel(dwh_conn, hoteles: List[Dict]):
    """Carga la dimensión de hoteles"""
    print(f"Cargando {len(hoteles)} hoteles a Dim_Hotel...")
    
    batch = [
        (h['hotel_id'], h['nombre'], h['direccion'], h['ciudad'], 
         h['pais'], h['categoria_estrellas'], h['numero_habitaciones_total'])
        for h in hoteles
    ]
    
    await dwh_conn.executemany("""
        INSERT INTO Dim_Hotel (
            hotel_id_erp, nombre, direccion, ciudad, pais,
            categoria_estrellas, numero_habitaciones_total
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (hotel_id_erp) DO UPDATE SET
            nombre = EXCLUDED.nombre,
            direccion = EXCLUDED.direccion,
            ciudad = EXCLUDED.ciudad,
            pais = EXCLUDED.pais,
            categoria_estrellas = EXCLUDED.categoria_estrellas,
            numero_habitaciones_total = EXCLUDED.numero_habitaciones_total,
            fecha_actualizacion = CURRENT_TIMESTAMP
    """, batch)
    
    print("Dim_Hotel cargada exitosamente")


async def extract_tipos_habitacion(erp_conn) -> List[Dict]:
    """Extrae tipos de habitación del ERP"""
    print("Extrayendo Tipos de Habitación del ERP...")
    rows = await erp_conn.fetch("SELECT * FROM TiposHabitacion")
    return [dict(row) for row in rows]


async def load_dim_tipo_habitacion(dwh_conn, tipos: List[Dict]):
    """Carga la dimensión de tipos de habitación"""
    print(f"Cargando {len(tipos)} tipos de habitación a Dim_TipoHabitacion...")
    
    batch = [
        (t['tipo_habitacion_id'], t['hotel_id'], t['nombre_tipo'],
         t['descripcion'], t['capacidad_maxima'], t['precio_base_noche'])
        for t in tipos
    ]
    
    await dwh_conn.executemany("""
        INSERT INTO Dim_TipoHabitacion (
            tipo_habitacion_id_erp, hotel_id_erp, nombre_tipo,
            descripcion, capacidad_maxima, precio_base_noche
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (tipo_habitacion_id_erp) DO UPDATE SET
            hotel_id_erp = EXCLUDED.hotel_id_erp,
            nombre_tipo = EXCLUDED.nombre_tipo,
            descripcion = EXCLUDED.descripcion,
            capacidad_maxima = EXCLUDED.capacidad_maxima,
            precio_base_noche = EXCLUDED.precio_base_noche,
            fecha_actualizacion = CURRENT_TIMESTAMP
    """, batch)
    
    print("Dim_TipoHabitacion cargada exitosamente")


async def extract_huespedes(erp_conn) -> List[Dict]:
    """Extrae huéspedes del ERP"""
    print("Extrayendo Huéspedes del ERP...")
    rows = await erp_conn.fetch("SELECT * FROM Huespedes")
    return [dict(row) for row in rows]


async def load_dim_huesped(dwh_conn, huespedes: List[Dict]):
    """Carga la dimensión de huéspedes"""
    print(f"Cargando {len(huespedes)} huéspedes a Dim_Huesped...")
    
    batch_size = 1000
    total = len(huespedes)
    
    for i in range(0, total, batch_size):
        batch = huespedes[i:i + batch_size]
        data = [
            (h['huesped_id'], h['nombre'], h['apellido'], h['email'], h['pais_origen'])
            for h in batch
        ]
        
        await dwh_conn.executemany("""
            INSERT INTO Dim_Huesped (
                huesped_id_erp, nombre, apellido, email, pais_origen
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (huesped_id_erp) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                apellido = EXCLUDED.apellido,
                email = EXCLUDED.email,
                pais_origen = EXCLUDED.pais_origen,
                fecha_actualizacion = CURRENT_TIMESTAMP
        """, data)
        
        print(f"  Procesados {min(i + batch_size, total)}/{total} huéspedes...")
    
    print("Dim_Huesped cargada exitosamente")


async def extract_reservas_con_pagos(erp_conn) -> List[Dict]:
    """Extrae reservas del ERP con pagos agregados"""
    print("Extrayendo Reservas y Pagos del ERP...")
    
    query = """
        SELECT 
            r.*,
            COALESCE(SUM(p.monto) FILTER (WHERE p.estado_pago = 'completado'), 0) as monto_pagado,
            COALESCE(SUM(cs.precio_total_consumo), 0) as monto_consumos
        FROM Reservas r
        LEFT JOIN Pagos p ON r.reserva_id = p.reserva_id
        LEFT JOIN ConsumosServicios cs ON r.reserva_id = cs.reserva_id
        GROUP BY r.reserva_id
        ORDER BY r.reserva_id
    """
    
    rows = await erp_conn.fetch(query)
    return [dict(row) for row in rows]


async def get_dimension_keys(dwh_conn) -> Dict[str, Dict]:
    """Obtiene mapeos de IDs del ERP a claves del DWH"""
    print("Obteniendo mapeos de dimensiones...")
    
    hoteles = await dwh_conn.fetch("SELECT hotel_key, hotel_id_erp FROM Dim_Hotel")
    tipos = await dwh_conn.fetch("SELECT tipo_habitacion_key, tipo_habitacion_id_erp FROM Dim_TipoHabitacion")
    canales = await dwh_conn.fetch("SELECT canal_key, canal_codigo FROM Dim_Canal")
    huespedes = await dwh_conn.fetch("SELECT huesped_key, huesped_id_erp FROM Dim_Huesped")
    tiempos = await dwh_conn.fetch("SELECT tiempo_id, fecha FROM Dim_Tiempo")
    
    return {
        'hoteles': {row['hotel_id_erp']: row['hotel_key'] for row in hoteles},
        'tipos': {row['tipo_habitacion_id_erp']: row['tipo_habitacion_key'] for row in tipos},
        'canales': {row['canal_codigo']: row['canal_key'] for row in canales},
        'huespedes': {row['huesped_id_erp']: row['huesped_key'] for row in huespedes},
        'tiempos': {row['fecha']: row['tiempo_id'] for row in tiempos}
    }


async def load_fact_reservas(dwh_conn, reservas: List[Dict], dimension_keys: Dict):
    """Carga la tabla de hechos de reservas"""
    print(f"Cargando {len(reservas)} reservas a Fact_Reservas...")
    
    loaded = 0
    skipped = 0
    batch_size = 500
    
    for i in range(0, len(reservas), batch_size):
        batch_reservas = reservas[i:i + batch_size]
        batch_data = []
        
        for reserva in batch_reservas:
            try:
                hotel_key = dimension_keys['hoteles'].get(reserva['hotel_id'])
                tipo_key = dimension_keys['tipos'].get(reserva['tipo_habitacion_id'])
                canal_key = dimension_keys['canales'].get(str(reserva['canal_reserva']))
                huesped_key = dimension_keys['huespedes'].get(reserva['huesped_id'])
                
                if not all([hotel_key, tipo_key, canal_key, huesped_key]):
                    skipped += 1
                    continue
                
                fecha_checkin_id = dimension_keys['tiempos'].get(reserva['fecha_checkin'])
                fecha_checkout_id = dimension_keys['tiempos'].get(reserva['fecha_checkout'])
                fecha_creacion_id = dimension_keys['tiempos'].get(reserva['fecha_creacion_reserva'].date())
                
                if not all([fecha_checkin_id, fecha_checkout_id, fecha_creacion_id]):
                    skipped += 1
                    continue
                
                noches = (reserva['fecha_checkout'] - reserva['fecha_checkin']).days
                
                if noches <= 0:
                    skipped += 1
                    continue
                
                batch_data.append((
                    reserva['reserva_id'], hotel_key, tipo_key, canal_key, huesped_key,
                    fecha_checkin_id, fecha_checkout_id, fecha_creacion_id,
                    reserva['monto_total_reserva'], reserva['monto_pagado'], reserva['monto_consumos'],
                    noches, reserva['numero_adultos'], reserva['numero_ninos'],
                    reserva['precio_total_noche'], str(reserva['estado_reserva'])
                ))
                
            except Exception as e:
                print(f"Error procesando reserva {reserva.get('reserva_id', 'unknown')}: {e}")
                skipped += 1
        
        if batch_data:
            await dwh_conn.executemany("""
                INSERT INTO Fact_Reservas (
                    reserva_id_erp, hotel_key, tipo_habitacion_key, canal_key, huesped_key,
                    fecha_checkin_id, fecha_checkout_id, fecha_creacion_id,
                    monto_total_reserva, monto_pagado, monto_consumos,
                    noches_estadia, numero_adultos, numero_ninos,
                    precio_total_noche, estado_reserva
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (reserva_id_erp) DO UPDATE SET
                    hotel_key = EXCLUDED.hotel_key,
                    tipo_habitacion_key = EXCLUDED.tipo_habitacion_key,
                    canal_key = EXCLUDED.canal_key,
                    huesped_key = EXCLUDED.huesped_key,
                    fecha_checkin_id = EXCLUDED.fecha_checkin_id,
                    fecha_checkout_id = EXCLUDED.fecha_checkout_id,
                    fecha_creacion_id = EXCLUDED.fecha_creacion_id,
                    monto_total_reserva = EXCLUDED.monto_total_reserva,
                    monto_pagado = EXCLUDED.monto_pagado,
                    monto_consumos = EXCLUDED.monto_consumos,
                    noches_estadia = EXCLUDED.noches_estadia,
                    numero_adultos = EXCLUDED.numero_adultos,
                    numero_ninos = EXCLUDED.numero_ninos,
                    precio_total_noche = EXCLUDED.precio_total_noche,
                    estado_reserva = EXCLUDED.estado_reserva,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """, batch_data)
            
            loaded += len(batch_data)
            print(f"  Procesadas {min(i + batch_size, len(reservas))}/{len(reservas)} reservas...")
    
    print(f"Fact_Reservas: {loaded} cargadas, {skipped} omitidas")


async def run_etl():
    """Ejecuta el proceso ETL completo"""
    print("=" * 60)
    print("INICIANDO PROCESO ETL")
    print("=" * 60)
    
    erp_conn = None
    dwh_conn = None
    
    try:
        print("\n[1/8] Conectando a bases de datos...")
        erp_conn = await get_erp_connection()
        dwh_conn = await get_dwh_connection()
        print("✓ Conexiones establecidas\n")
        
        print("[2/8] Poblando Dim_Tiempo...")
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2030, 12, 31)
        await populate_dim_tiempo(dwh_conn, start_date, end_date)
        print("✓ Dim_Tiempo completada\n")
        
        print("[3/8] Extrayendo y cargando Hoteles...")
        hoteles = await extract_hoteles(erp_conn)
        await load_dim_hotel(dwh_conn, hoteles)
        print("✓ Dim_Hotel completada\n")
        
        print("[4/8] Extrayendo y cargando Tipos de Habitación...")
        tipos = await extract_tipos_habitacion(erp_conn)
        await load_dim_tipo_habitacion(dwh_conn, tipos)
        print("✓ Dim_TipoHabitacion completada\n")
        
        print("[5/8] Extrayendo y cargando Huéspedes...")
        huespedes = await extract_huespedes(erp_conn)
        await load_dim_huesped(dwh_conn, huespedes)
        print("✓ Dim_Huesped completada\n")
        
        print("[6/8] Obteniendo mapeos de dimensiones...")
        dimension_keys = await get_dimension_keys(dwh_conn)
        print("✓ Mapeos obtenidos\n")
        
        print("[7/8] Extrayendo Reservas con Pagos y Consumos...")
        reservas = await extract_reservas_con_pagos(erp_conn)
        print(f"✓ {len(reservas)} reservas extraídas\n")
        
        print("[8/8] Cargando Fact_Reservas...")
        await load_fact_reservas(dwh_conn, reservas, dimension_keys)
        print("✓ Fact_Reservas completada\n")
        
        print("=" * 60)
        print("ETL COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ ERROR EN ETL: {e}")
        raise
        
    finally:
        if erp_conn:
            await erp_conn.close()
        if dwh_conn:
            await dwh_conn.close()
        print("\nConexiones cerradas")


if __name__ == "__main__":
    asyncio.run(run_etl())
