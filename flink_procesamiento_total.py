import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def ejecutar_procesamiento_total():
    print("Iniciando Motor Unificado de PyFlink (Corrección de Watermarks e Idle Partitions)...")
    
    # 1. CONFIGURACIÓN DEL ENTORNO
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_jar = "file:///home/ubuntu/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # =====================================================================
    # 2. DEFINICIÓN DE ORÍGENES (Corregido mapeo de 'ts' e Idle Timeout)
    # =====================================================================
    
    t_env.execute_sql("""
        CREATE TABLE origen_vistas (
            event_id STRING, 
            agent_id STRING, 
            event_type STRING, 
            ts STRING, -- Lee el campo exacto del JSON
            payload ROW<product_id STRING, latitud DOUBLE, longitud DOUBLE>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.ver_producto', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000' -- Evita congelamientos por particiones vacías
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE origen_compras (
            event_id STRING, 
            agent_id STRING, 
            ts STRING,
            payload ROW<product_id STRING, monto DOUBLE, latitud DOUBLE, longitud DOUBLE>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.compra', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE origen_pagos (
            event_id STRING, 
            ts STRING,
            payload ROW<estado STRING, monto DOUBLE>,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.pago', 'properties.bootstrap.servers' = 'localhost:9092', 
            'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset',
            'scan.watermark.idle-timeout' = '2000'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE origen_abandono (
            event_id STRING, 
            agent_id STRING, 
            ts STRING,
            evento_ts AS TO_TIMESTAMP(SUBSTRING(REPLACE(ts, 'T', ' '), 1, 23)),
            WATERMARK FOR evento_ts AS evento_ts - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.abandono', 'properties.bootstrap.servers' = 'localhost:9092', 
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

    # =====================================================================
    # 4. REGLAS DE NEGOCIO (Usando el nuevo campo `evento_ts`)
    # =====================================================================
    statement_set = t_env.create_statement_set()

    # Regla 1 (Tráfico)
    statement_set.add_insert_sql("""
        INSERT INTO destino_trafico
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '5' SECONDS) AS ventana_tiempo,
            'vistas_por_5_segundos' AS metrica,
            COUNT(event_id) AS valor
        FROM origen_vistas
        GROUP BY TUMBLE(evento_ts, INTERVAL '5' SECONDS)
    """)

    # Regla 2 (Negocio)
    statement_set.add_insert_sql("""
        INSERT INTO destino_negocio
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '15' SECONDS) AS ventana_tiempo, -- Reducido a 15s para visualización rápida
            SUM(payload.monto) AS total_ventas,
            COUNT(event_id) AS tickets
        FROM origen_compras
        GROUP BY TUMBLE(evento_ts, INTERVAL '15' SECONDS)
    """)

    # Regla 3 (Audiencias)
    statement_set.add_insert_sql("""
        INSERT INTO destino_audiencias
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '30' SECONDS) AS ventana_tiempo,
            agent_id,
            'Usuario Explorador' AS audiencia,
            CAST(COUNT(event_id) AS STRING) || ' vistas en 30s' AS justificacion
        FROM origen_vistas
        GROUP BY agent_id, TUMBLE(evento_ts, INTERVAL '30' SECONDS)
        HAVING COUNT(event_id) >= 8
    """)

    # Regla 4 (Alertas)
    statement_set.add_insert_sql("""
        INSERT INTO destino_alertas
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '10' SECONDS) AS ventana_tiempo,
            'CRÍTICO' AS nivel,
            'Anomalía: ' || CAST(COUNT(event_id) AS STRING) || ' pagos rechazados detectados' AS mensaje
        FROM origen_pagos
        WHERE payload.estado = 'fallido'
        GROUP BY TUMBLE(evento_ts, INTERVAL '10' SECONDS)
        HAVING COUNT(event_id) >= 4
    """)
    
    # Regla 5 (Geoespacial)
    statement_set.add_insert_sql("""
        INSERT INTO destino_geo
        SELECT 
            TUMBLE_START(evento_ts, INTERVAL '15' SECONDS) AS ventana_tiempo, -- Reducido a 15s para pintar el mapa rápido
            ROUND(payload.latitud, 2) AS latitud, 
            ROUND(payload.longitud, 2) AS longitud,
            SUM(payload.monto) AS total_ventas
        FROM origen_compras
        GROUP BY 
            ROUND(payload.latitud, 2), 
            ROUND(payload.longitud, 2), 
            TUMBLE(evento_ts, INTERVAL '15' SECONDS)
    """)

    # Regla 6 (Interval Join)
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
    print("Enviando Job (Event Time corregido)...")
    statement_set.execute()

if __name__ == '__main__':
    ejecutar_procesamiento_total()