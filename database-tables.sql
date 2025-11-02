DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'estado_habitacion_enum') THEN
        CREATE TYPE estado_habitacion_enum AS ENUM (
            'disponible', 
            'ocupada', 
            'mantenimiento', 
            'limpieza'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'estado_reserva_enum') THEN
        CREATE TYPE estado_reserva_enum AS ENUM (
            'pendiente', 
            'confirmada', 
            'cancelada', 
            'checkin', 
            'checkout', 
            'no-show'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'canal_reserva_enum') THEN
        CREATE TYPE canal_reserva_enum AS ENUM (
            'web_directa', 
            'booking_com', 
            'expedia', 
            'telefono', 
            'agencia'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'metodo_pago_enum') THEN
        CREATE TYPE metodo_pago_enum AS ENUM (
            'tarjeta_credito', 
            'transferencia', 
            'efectivo'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'estado_pago_enum') THEN
        CREATE TYPE estado_pago_enum AS ENUM (
            'completado', 
            'pendiente', 
            'fallido'
        );
    END IF;
END$$;

/* ================================================
 B. CREACIÓN DE TABLAS (IF NOT EXISTS)
================================================
*/


CREATE TABLE IF NOT EXISTS Hoteles (
    hotel_id BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    direccion TEXT,
    ciudad VARCHAR(100),
    pais VARCHAR(100),
    categoria_estrellas INTEGER CHECK (categoria_estrellas >= 1 AND categoria_estrellas <= 5),
    numero_habitaciones_total INTEGER
);

CREATE TABLE IF NOT EXISTS Huespedes (
    huesped_id BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    telefono VARCHAR(50),
    fecha_nacimiento DATE,
    pais_origen VARCHAR(60),
    fecha_registro TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS TiposHabitacion (
    tipo_habitacion_id BIGSERIAL PRIMARY KEY,
    hotel_id BIGINT NOT NULL REFERENCES Hoteles(hotel_id) ON DELETE CASCADE,
    nombre_tipo VARCHAR(100) NOT NULL,
    descripcion TEXT,
    capacidad_maxima INTEGER CHECK (capacidad_maxima > 0),
    precio_base_noche DECIMAL(10, 2) NOT NULL CHECK (precio_base_noche >= 0)
);

CREATE TABLE IF NOT EXISTS ServiciosAdicionales (
    servicio_id BIGSERIAL PRIMARY KEY,
    hotel_id BIGINT NOT NULL REFERENCES Hoteles(hotel_id) ON DELETE CASCADE,
    nombre_servicio VARCHAR(255) NOT NULL,
    precio DECIMAL(10, 2) NOT NULL CHECK (precio >= 0)
);

CREATE TABLE IF NOT EXISTS Habitaciones (
    habitacion_id BIGSERIAL PRIMARY KEY,
    hotel_id BIGINT NOT NULL REFERENCES Hoteles(hotel_id) ON DELETE RESTRICT,
    tipo_habitacion_id BIGINT NOT NULL REFERENCES TiposHabitacion(tipo_habitacion_id) ON DELETE RESTRICT,
    numero_habitacion VARCHAR(20) NOT NULL,
    estado_actual estado_habitacion_enum DEFAULT 'disponible',
    UNIQUE(hotel_id, numero_habitacion)
);

CREATE TABLE IF NOT EXISTS Reservas (
    reserva_id BIGSERIAL PRIMARY KEY,
    huesped_id BIGINT NOT NULL REFERENCES Huespedes(huesped_id) ON DELETE RESTRICT,
    hotel_id BIGINT NOT NULL REFERENCES Hoteles(hotel_id) ON DELETE RESTRICT,
    tipo_habitacion_id BIGINT NOT NULL REFERENCES TiposHabitacion(tipo_habitacion_id) ON DELETE RESTRICT,
    fecha_checkin DATE NOT NULL,
    fecha_checkout DATE NOT NULL,
    numero_adultos INTEGER NOT NULL CHECK (numero_adultos > 0),
    numero_ninos INTEGER DEFAULT 0,
    estado_reserva estado_reserva_enum DEFAULT 'pendiente',
    fecha_creacion_reserva TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    canal_reserva canal_reserva_enum,
    precio_total_noche DECIMAL(10, 2) NOT NULL,
    monto_total_reserva DECIMAL(12, 2) NOT NULL,
    CHECK (fecha_checkout > fecha_checkin)
);

CREATE TABLE IF NOT EXISTS Pagos (
    pago_id BIGSERIAL PRIMARY KEY,
    reserva_id BIGINT NOT NULL REFERENCES Reservas(reserva_id) ON DELETE RESTRICT,
    monto DECIMAL(12, 2) NOT NULL,
    fecha_pago TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    metodo_pago metodo_pago_enum NOT NULL,
    estado_pago estado_pago_enum DEFAULT 'pendiente'
);

CREATE TABLE IF NOT EXISTS ConsumosServicios (
    consumo_id BIGSERIAL PRIMARY KEY,
    reserva_id BIGINT NOT NULL REFERENCES Reservas(reserva_id) ON DELETE RESTRICT,
    servicio_id BIGINT NOT NULL REFERENCES ServiciosAdicionales(servicio_id) ON DELETE RESTRICT,
    cantidad INTEGER DEFAULT 1,
    precio_total_consumo DECIMAL(10, 2) NOT NULL,
    fecha_consumo TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reviews (
  review_id BIGSERIAL PRIMARY KEY,
  reserva_id BIGINT NOT NULL REFERENCES reservas(reserva_id) ON DELETE RESTRICT,
  huesped_id BIGINT NOT NULL REFERENCES huespedes(huesped_id) ON DELETE RESTRICT,
  hotel_id BIGINT NOT NULL REFERENCES hoteles(hotel_id) ON DELETE RESTRICT,
  rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
  title VARCHAR(255),
  review_text TEXT,
  language VARCHAR(10),
  fecha_review TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  label_text VARCHAR(20)
);
