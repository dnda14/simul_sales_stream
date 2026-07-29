import streamlit as st
import json
import time
from confluent_kafka import Consumer, KafkaError
import pandas as pd

# 1. Configuración inicial de la página
st.set_page_config(page_title="Dashboard E-Commerce", page_icon="📊", layout="wide")
st.title("📊 Monitor de Inteligencia de Negocio en Tiempo Real")
st.markdown("Procesamiento distribuido con Apache Flink & Kafka KRaft")

# 2. Creación de los espacios vacíos (Placeholders)
# Estos contenedores se actualizarán dinámicamente sin recargar la web
st.header("📈 Métricas Globales (Última actualización)")
col1, col2, col3 = st.columns(3)
placeholder_trafico = col1.empty()
placeholder_ventas = col2.empty()
placeholder_tickets = col3.empty()

st.markdown("---")
col_izq, col_der = st.columns(2)

with col_izq:
    st.subheader("👥 Últimas Audiencias Detectadas")
    placeholder_audiencias = st.empty()

with col_der:
    st.subheader("⚠️ Alertas del Sistema")
    placeholder_alertas = st.empty()

# Listas en memoria para mantener un historial corto en las tablas
historial_audiencias = []
historial_alertas = []

# 3. Configuración del Consumidor de Kafka
@st.cache_resource
def crear_consumidor():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamlit-dashboard-web',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([
        'dashboard.trafico', 
        'dashboard.negocio', 
        'dashboard.audiencias', 
        'dashboard.alertas'
    ])
    return consumer

consumidor = crear_consumidor()

# 4. Bucle principal de consumo en tiempo real
st.caption("Buscando eventos en el clúster...")

try:
    while True:
        msg = consumidor.poll(0.1) # Leer cada 100ms
        
        if msg is None:
            time.sleep(0.1)
            continue
        if msg.error():
            continue

        # Decodificar el JSON de Flink
        topic = msg.topic()
        try:
            record = json.loads(msg.value().decode('utf-8'))
            
            # Enrutar el dato a su componente visual correspondiente
            if topic == 'dashboard.trafico':
                valor = record.get('valor', 0)
                hora = record.get('ventana_tiempo', '')[11:19]
                placeholder_trafico.metric(label=f"Vistas (Últimos 5s) - {hora}", value=f"{valor} eventos")
                
            elif topic == 'dashboard.negocio':
                ventas = record.get('total_ventas', 0)
                tickets = record.get('tickets', 0)
                hora = record.get('ventana_tiempo', '')[11:19]
                placeholder_ventas.metric(label=f"Ingresos (Último min) - {hora}", value=f"${ventas:,.2f}")
                placeholder_tickets.metric(label="Tickets (Último min)", value=tickets)
                
            elif topic == 'dashboard.audiencias':
                # Agregar al principio de la lista y mantener máximo 10 registros
                historial_audiencias.insert(0, {
                    "Hora": record.get('ventana_tiempo', '')[11:19],
                    "Agente": record.get('agent_id', ''),
                    "Perfil": record.get('audiencia', ''),
                    "Detalle": record.get('justificacion', '')
                })
                historial_audiencias = historial_audiencias[:10]
                placeholder_audiencias.dataframe(pd.DataFrame(historial_audiencias), use_container_width=True, hide_index=True)
                
            elif topic == 'dashboard.alertas':
                nivel = record.get('nivel', 'INFO')
                mensaje = record.get('mensaje', '')
                hora = record.get('ventana_tiempo', '')[11:19]
                
                icono = "🚨" if nivel == "CRÍTICO" else "⚠️"
                historial_alertas.insert(0, {"Hora": hora, "Nivel": icono, "Alerta": mensaje})
                historial_alertas = historial_alertas[:5]
                placeholder_alertas.dataframe(pd.DataFrame(historial_alertas), use_container_width=True, hide_index=True)

        except json.JSONDecodeError:
            pass

except Exception as e:
    st.error(f"Error en el flujo de datos: {e}")