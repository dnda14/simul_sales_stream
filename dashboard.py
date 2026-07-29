import streamlit as st
import json
import time
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import altair as alt

# 1. Configuración inicial de la página
st.set_page_config(page_title="Centro de Comando E-Commerce", page_icon="📈", layout="wide")
st.title("📈 Centro de Comando: Operaciones en Tiempo Real")
st.markdown("Procesamiento distribuido: **Apache Flink** | Backbone: **Kafka KRaft** | Analytics: **Event Time & Watermarks**")

# 2. Creación de la cuadrícula de Placeholders (Inyección Dinámica)
st.markdown("### 📊 KPIs Globales")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
ph_kpi_vistas = kpi1.empty()
ph_kpi_ventas = kpi2.empty()
ph_kpi_tickets = kpi3.empty()
ph_kpi_conversion = kpi4.empty()

st.markdown("---")
st.markdown("### 📈 Tendencias en Vivo")
col_chart1, col_chart2 = st.columns(2)
ph_chart_trafico = col_chart1.empty()
ph_chart_ventas = col_chart2.empty()

st.markdown("---")
col_mapa, col_tablas = st.columns([1.2, 1]) # El mapa un poco más ancho

with col_mapa:
    st.markdown("### 🗺️ Mapa de Calor de Ventas")
    ph_mapa = st.empty()

with col_tablas:
    st.markdown("### ⚠️ Alertas Críticas")
    ph_alertas = st.empty()
    st.markdown("### 👥 Radar de Audiencias")
    ph_audiencias = st.empty()

# 3. Listas en memoria (Sliding Windows para no saturar RAM)
MAX_PUNTOS = 30
historial_trafico = []
historial_ventas = []
historial_geo = []
historial_alertas = []
historial_audiencias = []
tiempos_decision = [] # Para sacar el promedio

# 4. Configuración del Consumidor de Kafka
@st.cache_resource
def crear_consumidor():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dashboard-gerencial-web',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    # ¡Suscripción a los 6 tópicos!
    consumer.subscribe([
        'dashboard.trafico', 'dashboard.negocio', 
        'dashboard.audiencias', 'dashboard.alertas',
        'dashboard.conversiones', 'dashboard.geo'
    ])
    return consumer

consumidor = crear_consumidor()

# 5. Función Auxiliar para Gráficos
def renderizar_grafico(datos, x_col, y_col, color, titulo):
    if not datos: return None
    df = pd.DataFrame(datos)
    chart = alt.Chart(df).mark_area(opacity=0.6, color=color).encode(
        x=alt.X(f'{x_col}:N', title='Hora', sort=None), # N de Nominal para no reordenar ejes
        y=alt.Y(f'{y_col}:Q', title=titulo),
        tooltip=[x_col, y_col]
    ).properties(height=250)
    return chart

# 6. Bucle principal de consumo en tiempo real
st.caption("🟢 Escuchando eventos del clúster...")

try:
    while True:
        msg = consumidor.poll(0.1)
        
        if msg is None or msg.error():
            time.sleep(0.1)
            continue

        topic = msg.topic()
        try:
            record = json.loads(msg.value().decode('utf-8'))
            
            # Extraer hora limpia (HH:MM:SS) para los ejes X
            hora = record.get('ventana_tiempo', '')[11:19] if 'ventana_tiempo' in record else ''
            
            # --- TÓPICO: TRÁFICO ---
            if topic == 'dashboard.trafico':
                vistas = record.get('valor', 0)
                ph_kpi_vistas.metric(label="Visitas (Últimos 5s)", value=f"{vistas:,}")
                
                historial_trafico.append({"Hora": hora, "Vistas": vistas})
                historial_trafico = historial_trafico[-MAX_PUNTOS:]
                
                chart = renderizar_grafico(historial_trafico, "Hora", "Vistas", "#1f77b4", "Volumen de Vistas")
                ph_chart_trafico.altair_chart(chart, use_container_width=True)
                
            # --- TÓPICO: NEGOCIO ---
            elif topic == 'dashboard.negocio':
                ventas = record.get('total_ventas', 0)
                tickets = record.get('tickets', 0)
                ph_kpi_ventas.metric(label="Ingresos (Último min)", value=f"${ventas:,.2f}")
                ph_kpi_tickets.metric(label="Tickets (Último min)", value=tickets)
                
                historial_ventas.append({"Hora": hora, "Ingresos": ventas})
                historial_ventas = historial_ventas[-MAX_PUNTOS:]
                
                chart = renderizar_grafico(historial_ventas, "Hora", "Ingresos", "#2ca02c", "Ingresos ($)")
                ph_chart_ventas.altair_chart(chart, use_container_width=True)
                
            # --- TÓPICO: CONVERSIONES (Interval Join) ---
            elif topic == 'dashboard.conversiones':
                ttp = record.get('tiempo_decision_segundos', 0)
                tiempos_decision.append(ttp)
                tiempos_decision = tiempos_decision[-50:] # Promedio de los últimos 50
                avg_ttp = sum(tiempos_decision) / len(tiempos_decision)
                ph_kpi_conversion.metric(label="Time-to-Purchase (Promedio)", value=f"{avg_ttp:.1f} seg")
                
            # --- TÓPICO: GEOESPACIAL ---
            elif topic == 'dashboard.geo':
                # Renombramos para que st.map lo entienda nativamente
                historial_geo.append({
                    "latitude": record.get('latitud'),
                    "longitude": record.get('longitud'),
                    "ventas": record.get('total_ventas')
                })
                historial_geo = historial_geo[-100:] # Retener últimos 100 puntos
                df_geo = pd.DataFrame(historial_geo)
                ph_mapa.map(df_geo) 
                
            # --- TÓPICO: ALERTAS ---
            elif topic == 'dashboard.alertas':
                nivel = record.get('nivel', 'INFO')
                historial_alertas.insert(0, {
                    "Hora": hora, 
                    "Nivel": "🚨" if nivel == "CRÍTICO" else "⚠️", 
                    "Mensaje": record.get('mensaje', '')
                })
                historial_alertas = historial_alertas[:5]
                ph_alertas.dataframe(pd.DataFrame(historial_alertas), use_container_width=True, hide_index=True)

            # --- TÓPICO: AUDIENCIAS ---
            elif topic == 'dashboard.audiencias':
                historial_audiencias.insert(0, {
                    "Hora": hora,
                    "Agente": record.get('agent_id', ''),
                    "Perfil": record.get('audiencia', ''),
                })
                historial_audiencias = historial_audiencias[:6]
                ph_audiencias.dataframe(pd.DataFrame(historial_audiencias), use_container_width=True, hide_index=True)

        except json.JSONDecodeError:
            pass

except Exception as e:
    st.error(f"Error en el flujo de datos: {e}")