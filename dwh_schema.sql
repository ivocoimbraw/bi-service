-- ================================================
-- DATA WAREHOUSE SCHEMA (OLAP)
-- Esquema de Estrella para BI Hotelero
-- ================================================

-- ================================================
-- DIMENSIONES
-- ================================================

-- Dim_Tiempo: Tabla de calendario pre-calculada
CREATE TABLE IF NOT EXISTS Dim_Tiempo (
    tiempo_id BIGSERIAL PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    semestre INTEGER NOT NULL,
    dia_semana INTEGER NOT NULL, -- 0=Lunes, 6=Domingo
    nombre_dia_semana VARCHAR(20) NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    es_fin_semana BOOLEAN NOT NULL,
    es_festivo BOOLEAN DEFAULT FALSE,
    semana_anio INTEGER NOT NULL
);

CREATE INDEX idx_dim_tiempo_fecha ON Dim_Tiempo(fecha);
CREATE INDEX idx_dim_tiempo_anio_mes ON Dim_Tiempo(anio, mes);

-- Dim_Hotel: Información del hotel
CREATE TABLE IF NOT EXISTS Dim_Hotel (
    hotel_key BIGSERIAL PRIMARY KEY,
    hotel_id_erp BIGINT NOT NULL UNIQUE,
    nombre VARCHAR(255) NOT NULL,
    direccion TEXT,
    ciudad VARCHAR(100),
    pais VARCHAR(100),
    categoria_estrellas INTEGER,
    numero_habitaciones_total INTEGER,
    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_hotel_erp ON Dim_Hotel(hotel_id_erp);

-- Dim_TipoHabitacion: Información del tipo de habitación
CREATE TABLE IF NOT EXISTS Dim_TipoHabitacion (
    tipo_habitacion_key BIGSERIAL PRIMARY KEY,
    tipo_habitacion_id_erp BIGINT NOT NULL UNIQUE,
    hotel_id_erp BIGINT NOT NULL,
    nombre_tipo VARCHAR(100) NOT NULL,
    descripcion TEXT,
    capacidad_maxima INTEGER,
    precio_base_noche DECIMAL(10, 2),
    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_tipo_habitacion_erp ON Dim_TipoHabitacion(tipo_habitacion_id_erp);
CREATE INDEX idx_dim_tipo_habitacion_hotel ON Dim_TipoHabitacion(hotel_id_erp);

-- Dim_Canal: Información del canal de reserva
CREATE TABLE IF NOT EXISTS Dim_Canal (
    canal_key BIGSERIAL PRIMARY KEY,
    canal_codigo VARCHAR(50) NOT NULL UNIQUE,
    canal_nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_canal_codigo ON Dim_Canal(canal_codigo);

-- Dim_Huesped: Información del huésped
CREATE TABLE IF NOT EXISTS Dim_Huesped (
    huesped_key BIGSERIAL PRIMARY KEY,
    huesped_id_erp BIGINT NOT NULL UNIQUE,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    email VARCHAR(255),
    pais_origen VARCHAR(60),
    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_huesped_erp ON Dim_Huesped(huesped_id_erp);

-- ================================================
-- TABLA DE HECHOS
-- ================================================

-- Fact_Reservas: Tabla de hechos principal
CREATE TABLE IF NOT EXISTS Fact_Reservas (
    fact_reserva_id BIGSERIAL PRIMARY KEY,
    reserva_id_erp BIGINT NOT NULL UNIQUE,
    
    -- Claves foráneas a dimensiones
    hotel_key BIGINT NOT NULL REFERENCES Dim_Hotel(hotel_key),
    tipo_habitacion_key BIGINT NOT NULL REFERENCES Dim_TipoHabitacion(tipo_habitacion_key),
    canal_key BIGINT NOT NULL REFERENCES Dim_Canal(canal_key),
    huesped_key BIGINT NOT NULL REFERENCES Dim_Huesped(huesped_key),
    fecha_checkin_id BIGINT NOT NULL REFERENCES Dim_Tiempo(tiempo_id),
    fecha_checkout_id BIGINT NOT NULL REFERENCES Dim_Tiempo(tiempo_id),
    fecha_creacion_id BIGINT NOT NULL REFERENCES Dim_Tiempo(tiempo_id),
    
    -- Métricas (Measures)
    monto_total_reserva DECIMAL(12, 2) NOT NULL,
    monto_pagado DECIMAL(12, 2) DEFAULT 0,
    monto_consumos DECIMAL(12, 2) DEFAULT 0,
    noches_estadia INTEGER NOT NULL CHECK (noches_estadia > 0),
    numero_adultos INTEGER NOT NULL,
    numero_ninos INTEGER DEFAULT 0,
    precio_total_noche DECIMAL(10, 2) NOT NULL,
    
    -- Estado de la reserva
    estado_reserva VARCHAR(20) NOT NULL,
    
    -- Metadatos ETL
    fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas analíticas
CREATE INDEX idx_fact_reservas_hotel ON Fact_Reservas(hotel_key);
CREATE INDEX idx_fact_reservas_tipo_habitacion ON Fact_Reservas(tipo_habitacion_key);
CREATE INDEX idx_fact_reservas_canal ON Fact_Reservas(canal_key);
CREATE INDEX idx_fact_reservas_huesped ON Fact_Reservas(huesped_key);
CREATE INDEX idx_fact_reservas_checkin ON Fact_Reservas(fecha_checkin_id);
CREATE INDEX idx_fact_reservas_checkout ON Fact_Reservas(fecha_checkout_id);
CREATE INDEX idx_fact_reservas_creacion ON Fact_Reservas(fecha_creacion_id);
CREATE INDEX idx_fact_reservas_estado ON Fact_Reservas(estado_reserva);
CREATE INDEX idx_fact_reservas_erp ON Fact_Reservas(reserva_id_erp);

-- Índices compuestos para consultas comunes
CREATE INDEX idx_fact_reservas_hotel_fechas ON Fact_Reservas(hotel_key, fecha_checkin_id, fecha_checkout_id);
CREATE INDEX idx_fact_reservas_fechas_estado ON Fact_Reservas(fecha_checkin_id, fecha_checkout_id, estado_reserva);

-- ================================================
-- DATOS INICIALES: Canales de Reserva
-- ================================================

INSERT INTO Dim_Canal (canal_codigo, canal_nombre, descripcion) VALUES
    ('web_directa', 'Web Directa', 'Reserva directa a través del sitio web del hotel'),
    ('booking_com', 'Booking.com', 'Reserva a través de Booking.com'),
    ('expedia', 'Expedia', 'Reserva a través de Expedia'),
    ('telefono', 'Teléfono', 'Reserva telefónica directa'),
    ('agencia', 'Agencia de Viajes', 'Reserva a través de agencia de viajes')
ON CONFLICT (canal_codigo) DO NOTHING;
