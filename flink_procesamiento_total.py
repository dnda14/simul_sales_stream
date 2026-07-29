import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def ejecutar_procesamiento_total():
    print("Iniciando Motor Unificado de PyFlink (Regex Topics & Perfiles de Audiencia)...")
    
    # 1. CONFIGURACIÓN DEL ENTORNO
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_jar = "file:///home/ubuntu/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # =====================================================================
    # 2. DEFINICIÓN DE ORÍGENES 
    # =====================================================================
    
    # NUEVO: Tabla Global que lee absolutamente TODOS los tópicos 'store.*' mediante Regex
    # Perfecto para métricas de alto nivel (canales y event_types)
    t_env.execute_sql("""
        CREATE TABLE origen_global (
            event_id STRING, 
            channel STRING,
            event_type STRING,
            ts STRING,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 
            'topic-pattern' = 'store\..*',  -- Expresión regular para múltiples tópicos
            'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-global-group', 
            'format' = 'json', 
            'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)

    # Actualizamos los esquemas para extraer el 'perfil_audiencia' inyectado
    t_env.execute_sql("""
        CREATE TABLE origen_vistas (
            event_id STRING, agent_id STRING, ts STRING,
            payload ROW<product_id STRING, latitud DOUBLE, longitud DOUBLE, perfil_audiencia STRING>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.ver_producto', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE origen_compras (
            event_id STRING, agent_id STRING, ts STRING,
            payload ROW<product_id STRING, monto DOUBLE, latitud DOUBLE, longitud DOUBLE, perfil_audiencia STRING>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.compra', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)

    # Para alertas y abandonos usamos tablas estándar
    t_env.execute_sql("""
        CREATE TABLE origen_pagos (
            event_id STRING, ts STRING,
            payload ROW<estado STRING, monto DOUBLE>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.pago', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)

    # =====================================================================
    # 3. DEFINICIÓN DE DESTINOS
    # =====================================================================
    
    t_env.execute_sql("CREATE TABLE destino_trafico ( ventana_tiempo TIMESTAMP(3), metrica STRING, valor BIGINT ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.trafico', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_negocio ( ventana_tiempo TIMESTAMP(3), total_ventas DOUBLE, tickets BIGINT ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.negocio', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_audiencias ( ventana_tiempo TIMESTAMP(3), agent_id STRING, audiencia STRING, justificacion STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.audiencias', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_alertas ( ventana_tiempo TIMESTAMP(3), nivel STRING, mensaje STRING ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.alertas', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_conversiones ( agent_id STRING, product_id STRING, tiempo_decision_segundos BIGINT ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.conversiones', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_geo ( ventana_tiempo TIMESTAMP(3), latitud DOUBLE, longitud DOUBLE, total_ventas DOUBLE ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.geo', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    
    # NUEVOS TÓPICOS DE AGRUPACIÓN
    t_env.execute_sql("CREATE TABLE destino_canales ( ventana_tiempo TIMESTAMP(3), canal STRING, total_eventos BIGINT ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.canales', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")
    t_env.execute_sql("CREATE TABLE destino_eventos ( ventana_tiempo TIMESTAMP(3), tipo_evento STRING, total_eventos BIGINT ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.eventos', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )")

    # =====================================================================
    # 4. REGLAS DE NEGOCIO Y AGREGACIONES
    # =====================================================================
    statement_set = t_env.create_statement_set()

    # NUEVA Regla 1: Distribución por Canal (Ventanas de 10 segundos)
    statement_set.add_insert_sql("""
        INSERT INTO destino_canales
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '10' SECONDS) AS ventana_tiempo,
            channel AS canal,
            COUNT(event_id) AS total_eventos
        FROM origen_global
        GROUP BY channel, TUMBLE(evento_ts, INTERVAL '10' SECONDS)
    """)

    # NUEVA Regla 2: Distribución por Tipo de Evento (Ventanas de 10 segundos)
    statement_set.add_insert_sql("""
        INSERT INTO destino_eventos
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '10' SECONDS) AS ventana_tiempo,
            event_type AS tipo_evento,
            COUNT(event_id) AS total_eventos
        FROM origen_global
        GROUP BY event_type, TUMBLE(evento_ts, INTERVAL '10' SECONDS)
    """)

    # Regla 3: Tráfico General
    statement_set.add_insert_sql("""
        INSERT INTO destino_trafico
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '5' SECONDS) AS ventana_tiempo,
            'eventos_por_5_segundos' AS metrica,
            COUNT(event_id) AS valor
        FROM origen_global
        GROUP BY TUMBLE(evento_ts, INTERVAL '5' SECONDS)
    """)

    # Regla 4: Negocio (Ventas)
    statement_set.add_insert_sql("""
        INSERT INTO destino_negocio
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '15' SECONDS) AS ventana_tiempo,
            SUM(payload.monto) AS total_ventas,
            COUNT(event_id) AS tickets
        FROM origen_compras
        GROUP BY TUMBLE(evento_ts, INTERVAL '15' SECONDS)
    """)

    # MODIFICADA Regla 5: Audiencias Dinámicas
    # Ahora detecta qué perfiles están aportando al volumen de compras en vivo
    statement_set.add_insert_sql("""
        INSERT INTO destino_audiencias
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '15' SECONDS) AS ventana_tiempo,
            'Global' AS agent_id,
            payload.perfil_audiencia AS audiencia,
            CAST(COUNT(event_id) AS STRING) || ' compras en 15s' AS justificacion
        FROM origen_compras
        GROUP BY payload.perfil_audiencia, TUMBLE(evento_ts, INTERVAL '15' SECONDS)
    """)

    # Regla 6: Alertas por fallos en pagos
    statement_set.add_insert_sql("""
        INSERT INTO destino_alertas
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '10' SECONDS) AS ventana_tiempo,
            'CRÍTICO' AS nivel,
            'Pico de rechazos: ' || CAST(COUNT(event_id) AS STRING) || ' pagos fallidos' AS mensaje
        FROM origen_pagos
        WHERE payload.estado = 'fallido'
        GROUP BY TUMBLE(evento_ts, INTERVAL '10' SECONDS)
        HAVING COUNT(event_id) >= 5
    """)
    
    # Regla 7: Mapa de Calor (Geoespacial)
    statement_set.add_insert_sql("""
        INSERT INTO destino_geo
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '10' SECONDS) AS ventana_tiempo,
            ROUND(payload.latitud, 1) AS latitud, -- Redondeo a 1 decimal para agrupar mejor las zonas
            ROUND(payload.longitud, 1) AS longitud,
            SUM(payload.monto) AS total_ventas
        FROM origen_compras
        GROUP BY 
            ROUND(payload.latitud, 1), 
            ROUND(payload.longitud, 1), 
            TUMBLE(evento_ts, INTERVAL '10' SECONDS)
    """)

    # Regla 8: Interval Join (Conversiones)
    statement_set.add_insert_sql("""
        INSERT INTO destino_conversiones
        SELECT 
            v.agent_id,
            v.payload.product_id,
            TIMESTAMPDIFF(SECOND, v.evento_ts, c.evento_ts) AS tiempo_decision_segundos
        FROM origen_vistas v
        JOIN origen_compras c 
          ON v.agent_id = c.agent_id 
          AND v.payload.product_id = c.payload.product_id
        WHERE c.evento_ts BETWEEN v.evento_ts AND v.evento_ts + INTERVAL '5' MINUTE
    """)

    # 5. EJECUTAR EL GRAFO
    print("Enviando Job unificado (Métricas de canales, eventos y perfiles)...")
    statement_set.execute()

if __name__ == '__main__':
    ejecutar_procesamiento_total()