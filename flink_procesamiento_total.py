import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def ejecutar_procesamiento_total():
    print("Iniciando Motor Unificado de PyFlink...")
    
    # 1. CONFIGURACIÓN DEL ENTORNO
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_jar = "file:///home/ubuntu/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # 2. DEFINICIÓN DE ORÍGENES (SOURCES) - Leyendo los JSON de Kafka
    # =====================================================================
    
    t_env.execute_sql("""
        CREATE TABLE origen_vistas (
            event_id STRING, agent_id STRING, event_type STRING, proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.ver_producto', 'properties.bootstrap.servers' = 'localhost:9092', 'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE origen_compras (
            event_id STRING, agent_id STRING, payload ROW<product_id STRING, monto DOUBLE>, proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.compra', 'properties.bootstrap.servers' = 'localhost:9092', 'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE origen_pagos (
            event_id STRING, payload ROW<estado STRING, monto DOUBLE>, proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.pago', 'properties.bootstrap.servers' = 'localhost:9092', 'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE origen_abandono (
            event_id STRING, agent_id STRING, proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka', 'topic' = 'store.abandono', 'properties.bootstrap.servers' = 'localhost:9092', 'properties.group.id' = 'flink-master-group', 'format' = 'json', 'scan.startup.mode' = 'latest-offset'
        )
    """)

    # 3. DEFINICIÓN DE DESTINOS (SINKS) - Escribiendo JSON procesados para Streamlit
    # =====================================================================
    
    t_env.execute_sql("""
        CREATE TABLE destino_trafico (
            ventana_tiempo TIMESTAMP(3), metrica STRING, valor BIGINT
        ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.trafico', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )
    """)

    t_env.execute_sql("""
        CREATE TABLE destino_negocio (
            ventana_tiempo TIMESTAMP(3), total_ventas DOUBLE, tickets BIGINT
        ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.negocio', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )
    """)

    t_env.execute_sql("""
        CREATE TABLE destino_audiencias (
            ventana_tiempo TIMESTAMP(3), agent_id STRING, audiencia STRING, justificacion STRING
        ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.audiencias', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )
    """)

    t_env.execute_sql("""
        CREATE TABLE destino_alertas (
            ventana_tiempo TIMESTAMP(3), nivel STRING, mensaje STRING
        ) WITH ( 'connector' = 'kafka', 'topic' = 'dashboard.alertas', 'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json' )
    """)

    # 4. REGLAS DE NEGOCIO Y PROCESAMIENTO (STATEMENT SET)
    # =====================================================================
    # Envolvemos todas las consultas en un solo StatementSet para usar un solo Job
    
    statement_set = t_env.create_statement_set()

    # Regla 1 (Tráfico): Medir eventos por segundo (Throughput) de visualizaciones
    statement_set.add_insert_sql("""
        INSERT INTO destino_trafico
        SELECT 
            TUMBLE_START(proctime, INTERVAL '5' SECONDS) AS ventana_tiempo,
            'vistas_por_5_segundos' AS metrica,
            COUNT(event_id) AS valor
        FROM origen_vistas
        GROUP BY TUMBLE(proctime, INTERVAL '5' SECONDS)
    """)

    # Regla 2 (Negocio): Agregación financiera (Revenue y Tickets por minuto)
    statement_set.add_insert_sql("""
        INSERT INTO destino_negocio
        SELECT 
            TUMBLE_START(proctime, INTERVAL '60' SECONDS) AS ventana_tiempo,
            SUM(payload.monto) AS total_ventas,
            COUNT(event_id) AS tickets
        FROM origen_compras
        GROUP BY TUMBLE(proctime, INTERVAL '60' SECONDS)
    """)

    # Regla 3 (Audiencias): Detectar "Usuario Explorador" (Alta navegación sin compra inmediata)
    statement_set.add_insert_sql("""
        INSERT INTO destino_audiencias
        SELECT 
            TUMBLE_START(proctime, INTERVAL '30' SECONDS) AS ventana_tiempo,
            agent_id,
            'Usuario Explorador' AS audiencia,
            CAST(COUNT(event_id) AS STRING) || ' vistas en 30s' AS justificacion
        FROM origen_vistas
        GROUP BY agent_id, TUMBLE(proctime, INTERVAL '30' SECONDS)
        HAVING COUNT(event_id) >= 8
    """)

    # Regla 4 (Audiencias): Detectar "Cliente Premium" (Ticket muy alto)
    # (Al ser una alerta instantánea, no requiere ventana de tiempo, pero se la ponemos de 1 segundo para emparejar formatos)
    statement_set.add_insert_sql("""
        INSERT INTO destino_audiencias
        SELECT 
            TUMBLE_START(proctime, INTERVAL '1' SECOND) AS ventana_tiempo,
            agent_id,
            'Cliente Premium' AS audiencia,
            'Compra de ' || CAST(payload.monto AS STRING) AS justificacion
        FROM origen_compras
        WHERE payload.monto > 2000.0
        GROUP BY agent_id, payload.monto, TUMBLE(proctime, INTERVAL '1' SECOND)
    """)

    # Regla 5 (Alertas): Detección de Anomalías - Picos de pagos rechazados
    statement_set.add_insert_sql("""
        INSERT INTO destino_alertas
        SELECT 
            TUMBLE_START(proctime, INTERVAL '10' SECONDS) AS ventana_tiempo,
            'CRÍTICO' AS nivel,
            'Anomalía: ' || CAST(COUNT(event_id) AS STRING) || ' pagos rechazados detectados' AS mensaje
        FROM origen_pagos
        WHERE payload.estado = 'fallido'
        GROUP BY TUMBLE(proctime, INTERVAL '10' SECONDS)
        HAVING COUNT(event_id) >= 4
    """)
    
    # Regla 6 (Alertas): Picos de Carritos Abandonados
    statement_set.add_insert_sql("""
        INSERT INTO destino_alertas
        SELECT 
            TUMBLE_START(proctime, INTERVAL '10' SECONDS) AS ventana_tiempo,
            'ADVERTENCIA' AS nivel,
            'Alta tasa de abandono: ' || CAST(COUNT(event_id) AS STRING) || ' abandonos' AS mensaje
        FROM origen_abandono
        GROUP BY TUMBLE(proctime, INTERVAL '10' SECONDS)
        HAVING COUNT(event_id) >= 15
    """)

    # 5. EJECUTAR EL GRAFO DE PROCESAMIENTO
    print("Enviando el Job distribuido a los Trabajadores de Flink...")
    statement_set.execute()

if __name__ == '__main__':
    ejecutar_procesamiento_total()