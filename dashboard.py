import streamlit as st
import json
import time
from confluent_kafka import Consumer
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
st.markdown("### 🎯 Atribución y Embudo de Conversión")
col_agrup1, col_agrup2 = st.columns(2)
with col_agrup1:
    ph_canales = st.empty()
with col_agrup2:
    ph_eventos = st.empty()

st.markdown("---")
col_mapa, col_tablas = st.columns([1.2, 1]) 

with col_mapa:
    st.markdown("### 🗺️ Mapa de Calor de Ventas")
    ph_mapa = st.empty()

with col_tablas:
    st.markdown("### ⚠️ Alertas Críticas")
    ph_alertas = st.empty()
    st.markdown("### 👥 Radar de Audiencias Dinámicas")
    ph_audiencias = st.empty()

# 3. Estructuras de datos en memoria (Sliding Windows y Agrupaciones)
MAX_PUNTOS = 30
historial_trafico = []
historial_ventas = []
historial_geo = []
historial_alertas = []
historial_audiencias = []
tiempos_decision = [] 

# Diccionarios para mantener el último estado de las métricas agrupadas
estado_canales = {}
estado_eventos = {}

# 4. Configuración del Consumidor de Kafka
@st.cache_resource
def crear_consumidor():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dashboard-gerencial-web-final',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    # Suscripción a los 8 tópicos de métricas
    consumer.subscribe([
        'dashboard.trafico', 'dashboard.negocio', 
        'dashboard.audiencias', 'dashboard.alertas',
        'dashboard.conversiones', 'dashboard.geo',
        'dashboard.canales', 'dashboard.eventos'
    ])
    return consumer

consumidor = crear_consumidor()

# 5. Funciones Auxiliares para Gráficos
def renderizar_grafico_area(datos, x_col, y_col, color, titulo):
    if not datos: return None
    df = pd.DataFrame(datos)
    return alt.Chart(df).mark_area(opacity=0.6, color=color).encode(
        x=alt.X(f'{x_col}:N', title='Hora', sort=None), 
        y=alt.Y(f'{y_col}:Q', title=titulo),
        tooltip=[x_col, y_col]
    ).properties(height=220)

def renderizar_dona(diccionario, titulo):
    if not diccionario: return None
    df = pd.DataFrame(list(diccionario.items()), columns=['Categoría', 'Valor'])
    chart = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="Valor", type="quantitative"),
        color=alt.Color(field="Categoría", type="nominal", legend=alt.Legend(title="Canales")),
        tooltip=['Categoría', 'Valor']
    ).properties(height=250, title=titulo)
    return chart

def renderizar_barras_horizontales(diccionario, titulo):
    if not diccionario: return None
    df = pd.DataFrame(list(diccionario.items()), columns=['Evento', 'Volumen']).sort_values('Volumen', ascending=False)
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('Volumen:Q', title='Cantidad'),
        y=alt.Y('Evento:N', sort='-x', title=''),
        color=alt.Color('Evento:N', legend=None),
        tooltip=['Evento', 'Volumen']
    ).properties(height=250, title=titulo)
    return chart

# 6. Bucle principal de consumo
st.caption("🟢 Escuchando eventos del clúster...")

try:
    while True:
        msg = consumidor.poll(0.1)
        
        if msg is None or msg.error():
            time.sleep(0.05) # Evita desconexión del WebSocket
            continue

        topic = msg.topic()
        try:
            record = json.loads(msg.value().decode('utf-8'))
            hora = record.get('ventana_tiempo', '')[11:19] if 'ventana_tiempo' in record else ''
            
            # --- TRÁFICO ---
            if topic == 'dashboard.trafico':
                vistas = record.get('valor', 0)
                ph_kpi_vistas.metric(label="Visitas (Últimos 5s)", value=f"{vistas:,}")
                historial_trafico.append({"Hora": hora, "Vistas": vistas})
                historial_trafico = historial_trafico[-MAX_PUNTOS:]
                chart = renderizar_grafico_area(historial_trafico, "Hora", "Vistas", "#1f77b4", "Volumen de Vistas")
                ph_chart_trafico.altair_chart(chart, use_container_width=True)
                
            # --- NEGOCIO ---
            elif topic == 'dashboard.negocio':
                ventas = record.get('total_ventas', 0)
                tickets = record.get('tickets', 0)
                ph_kpi_ventas.metric(label="Ingresos (Último min)", value=f"${ventas:,.2f}")
                ph_kpi_tickets.metric(label="Tickets (Último min)", value=tickets)
                historial_ventas.append({"Hora": hora, "Ingresos": ventas})
                historial_ventas = historial_ventas[-MAX_PUNTOS:]
                chart = renderizar_grafico_area(historial_ventas, "Hora", "Ingresos", "#2ca02c", "Ingresos ($)")
                ph_chart_ventas.altair_chart(chart, use_container_width=True)
                
            # --- CANALES Y EVENTOS (NUEVO) ---
            elif topic == 'dashboard.canales':
                estado_canales[record.get('canal', 'N/A')] = record.get('total_eventos', 0)
                chart_canales = renderizar_dona(estado_canales, "Distribución por Canal")
                ph_canales.altair_chart(chart_canales, use_container_width=True)

            elif topic == 'dashboard.eventos':
                estado_eventos[record.get('tipo_evento', 'N/A')] = record.get('total_eventos', 0)
                chart_eventos = renderizar_barras_horizontales(estado_eventos, "Volumen por Tipo de Evento")
                ph_eventos.altair_chart(chart_eventos, use_container_width=True)

            # --- CONVERSIONES ---
            elif topic == 'dashboard.conversiones':
                ttp = record.get('tiempo_decision_segundos', 0)
                tiempos_decision.append(ttp)
                tiempos_decision = tiempos_decision[-50:] 
                avg_ttp = sum(tiempos_decision) / len(tiempos_decision)
                ph_kpi_conversion.metric(label="Time-to-Purchase (Promedio)", value=f"{avg_ttp:.1f} seg")
                
            # --- GEOESPACIAL ---
            elif topic == 'dashboard.geo':
                historial_geo.append({
                    "latitude": record.get('latitud'),
                    "longitude": record.get('longitud'),
                    "ventas": record.get('total_ventas')
                })
                historial_geo = historial_geo[-100:] 
                ph_mapa.map(pd.DataFrame(historial_geo)) 
                
            # --- ALERTAS ---
            elif topic == 'dashboard.alertas':
                nivel = record.get('nivel', 'INFO')
                historial_alertas.insert(0, {
                    "Hora": hora, 
                    "Nivel": "🚨" if nivel == "CRÍTICO" else "⚠️", 
                    "Mensaje": record.get('mensaje', '')
                })
                historial_alertas = historial_alertas[:5]
                ph_alertas.dataframe(pd.DataFrame(historial_alertas), use_container_width=True, hide_index=True)

            # --- AUDIENCIAS ---
            elif topic == 'dashboard.audiencias':
                historial_audiencias.insert(0, {
                    "Hora": hora,
                    "Perfil Activo": record.get('audiencia', ''),
                    "Actividad": record.get('justificacion', '')
                })
                historial_audiencias = historial_audiencias[:6]
                ph_audiencias.dataframe(pd.DataFrame(historial_audiencias), use_container_width=True, hide_index=True)

        except json.JSONDecodeError:
            pass
            
        time.sleep(0.05)

except Exception as e:
    st.error(f"Error en el flujo de datos: {e}")