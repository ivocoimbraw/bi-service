import strawberry
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
from database import get_connection


@strawberry.type
class HotelAnalytics:
    """Tipo de respuesta con KPIs de análisis hotelero"""
    
    hotel_id_erp: Optional[int] = None
    hotel_nombre: Optional[str] = None
    fecha_inicio: date
    fecha_fin: date
    
    total_reservas: int
    total_noches_vendidas: int
    total_noches_disponibles: int
    ingresos_totales_habitaciones: float
    
    tasa_ocupacion: float
    adr: float
    revpar: float
    
    reservas_por_canal: List['ReservasPorCanal']
    reservas_por_estado: List['ReservasPorEstado']


@strawberry.type
class ReservasPorCanal:
    """Distribución de reservas por canal"""
    canal_nombre: str
    cantidad_reservas: int
    ingresos_totales: float
    porcentaje: float


@strawberry.type
class ReservasPorEstado:
    """Distribución de reservas por estado"""
    estado: str
    cantidad: int
    porcentaje: float


async def calcular_kpis(
    fecha_inicio: date,
    fecha_fin: date,
    hotel_id_erp: Optional[int] = None
) -> HotelAnalytics:
    """Calcula los KPIs principales de BI"""
    
    async with get_connection() as conn:
        base_query = """
            WITH reservas_periodo AS (
                SELECT 
                    fr.*,
                    dh.hotel_id_erp,
                    dh.nombre as hotel_nombre,
                    dh.numero_habitaciones_total,
                    dc.canal_nombre,
                    dt_checkin.fecha as fecha_checkin,
                    dt_checkout.fecha as fecha_checkout
                FROM Fact_Reservas fr
                JOIN Dim_Hotel dh ON fr.hotel_key = dh.hotel_key
                JOIN Dim_Canal dc ON fr.canal_key = dc.canal_key
                JOIN Dim_Tiempo dt_checkin ON fr.fecha_checkin_id = dt_checkin.tiempo_id
                JOIN Dim_Tiempo dt_checkout ON fr.fecha_checkout_id = dt_checkout.tiempo_id
                WHERE dt_checkin.fecha >= $1 
                  AND dt_checkout.fecha <= $2
                  AND fr.estado_reserva IN ('confirmada', 'checkin', 'checkout')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if hotel_id_erp:
            base_query += " AND dh.hotel_id_erp = $3"
            params.append(hotel_id_erp)
        
        base_query += ")"
        
        # KPIs principales
        kpis_query = base_query + """
            SELECT 
                COUNT(*) as total_reservas,
                SUM(noches_estadia) as total_noches_vendidas,
                SUM(monto_total_reserva) as ingresos_totales,
                MAX(hotel_id_erp) as hotel_id,
                MAX(hotel_nombre) as hotel_nombre,
                MAX(numero_habitaciones_total) as num_habitaciones
            FROM reservas_periodo
        """
        
        kpis = await conn.fetchrow(kpis_query, *params)
        
        if not kpis or kpis['total_reservas'] == 0:
            return HotelAnalytics(
                hotel_id_erp=hotel_id_erp,
                hotel_nombre=None,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                total_reservas=0,
                total_noches_vendidas=0,
                total_noches_disponibles=0,
                ingresos_totales_habitaciones=0.0,
                tasa_ocupacion=0.0,
                adr=0.0,
                revpar=0.0,
                reservas_por_canal=[],
                reservas_por_estado=[]
            )
        
        # Calcular noches disponibles
        dias_periodo = (fecha_fin - fecha_inicio).days + 1
        
        if hotel_id_erp:
            total_noches_disponibles = kpis['num_habitaciones'] * dias_periodo
        else:
            total_hoteles_query = """
                SELECT SUM(numero_habitaciones_total) as total_habitaciones
                FROM Dim_Hotel
            """
            total_hab = await conn.fetchrow(total_hoteles_query)
            total_noches_disponibles = total_hab['total_habitaciones'] * dias_periodo
        
        # Calcular KPIs
        total_noches_vendidas = kpis['total_noches_vendidas'] or 0
        ingresos_totales = float(kpis['ingresos_totales'] or 0)
        
        tasa_ocupacion = (total_noches_vendidas / total_noches_disponibles * 100) if total_noches_disponibles > 0 else 0
        adr = (ingresos_totales / total_noches_vendidas) if total_noches_vendidas > 0 else 0
        revpar = (ingresos_totales / total_noches_disponibles) if total_noches_disponibles > 0 else 0
        
        # Reservas por canal
        canal_query = base_query + """
            SELECT 
                canal_nombre,
                COUNT(*) as cantidad,
                SUM(monto_total_reserva) as ingresos
            FROM reservas_periodo
            GROUP BY canal_nombre
            ORDER BY cantidad DESC
        """
        
        canales = await conn.fetch(canal_query, *params)
        total_reservas = kpis['total_reservas']
        
        reservas_por_canal = [
            ReservasPorCanal(
                canal_nombre=c['canal_nombre'],
                cantidad_reservas=c['cantidad'],
                ingresos_totales=float(c['ingresos'] or 0),
                porcentaje=round(c['cantidad'] / total_reservas * 100, 2) if total_reservas > 0 else 0
            )
            for c in canales
        ]
        
        # Reservas por estado
        estado_query = base_query + """
            SELECT 
                estado_reserva,
                COUNT(*) as cantidad
            FROM reservas_periodo
            GROUP BY estado_reserva
            ORDER BY cantidad DESC
        """
        
        estados = await conn.fetch(estado_query, *params)
        
        reservas_por_estado = [
            ReservasPorEstado(
                estado=e['estado_reserva'],
                cantidad=e['cantidad'],
                porcentaje=round(e['cantidad'] / total_reservas * 100, 2) if total_reservas > 0 else 0
            )
            for e in estados
        ]
        
        return HotelAnalytics(
            hotel_id_erp=kpis['hotel_id'],
            hotel_nombre=kpis['hotel_nombre'],
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            total_reservas=total_reservas,
            total_noches_vendidas=total_noches_vendidas,
            total_noches_disponibles=total_noches_disponibles,
            ingresos_totales_habitaciones=ingresos_totales,
            tasa_ocupacion=round(tasa_ocupacion, 2),
            adr=round(adr, 2),
            revpar=round(revpar, 2),
            reservas_por_canal=reservas_por_canal,
            reservas_por_estado=reservas_por_estado
        )


@strawberry.type
class Query:
    @strawberry.field
    async def hotel_analytics(
        self,
        fecha_inicio: date,
        fecha_fin: date,
        hotel_id_erp: Optional[int] = None
    ) -> HotelAnalytics:
        """
        Consulta principal de analytics hotelero.
        
        Args:
            fecha_inicio: Fecha de inicio del periodo (inclusive)
            fecha_fin: Fecha de fin del periodo (inclusive)
            hotel_id_erp: ID del hotel en el ERP (opcional, si no se especifica analiza todos)
        
        Returns:
            HotelAnalytics con KPIs calculados
        """
        return await calcular_kpis(fecha_inicio, fecha_fin, hotel_id_erp)


# Usar schema con soporte de Apollo Federation
schema = strawberry.federation.Schema(query=Query, enable_federation_2=True)
