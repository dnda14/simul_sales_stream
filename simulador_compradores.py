import json
import time
import uuid
import random
import argparse
from datetime import datetime, timedelta
from multiprocessing import Process, Value
from confluent_kafka import Producer

# ==========================================
# 1. GEOGRAFÍA LIMITADA POR BOUNDING BOXES
# ==========================================
PAISES = {
    'Peru':     {'lat': (-18.3, -0.0), 'lon': (-81.3, -68.6)},
    'Argentina':{'lat': (-55.0, -21.7), 'lon': (-73.5, -53.6)},
    'Chile':    {'lat': (-56.0, -17.5), 'lon': (-75.6, -66.9)},
    'Brasil':   {'lat': (-33.7, 5.2),  'lon': (-73.9, -34.7)},
    'USA':      {'lat': (24.3, 49.3),  'lon': (-125.0, -66.9)},
    'Espana':   {'lat': (36.0, 43.7),  'lon': (-9.3, 3.3)},
    'Colombia': {'lat': (-4.2, 12.4),  'lon': (-79.0, -67.1)},
    'Mexico':   {'lat': (14.5, 32.7),  'lon': (-118.3, -86.7)}
}

def generar_coordenadas(tipo_evento):
    if tipo_evento == 3 and random.random() < 0.85: # Fiestas Patrias (85% tráfico de Perú)
        pais = 'Peru'
    else:
        pais = random.choice(list(PAISES.keys()))
    
    bounds = PAISES[pais]
    lat = round(random.uniform(bounds['lat'][0], bounds['lat'][1]), 6)
    lon = round(random.uniform(bounds['lon'][0], bounds['lon'][1]), 6)
    return lat, lon

# ==========================================
# 2. DEFINICIÓN DE PERFILES Y MARKOV DINÁMICO
# ==========================================
TIPOS_PERFIL = ['Impulsivo', 'Abandonador', 'Cazador de Ofertas', 'Vitrinero']
PESOS_PERFIL = [0.15, 0.25, 0.25, 0.35]

# Cadenas de Markov específicas por audiencia
TRANSICIONES_PERFIL = {
    'Impulsivo': {
        'OFFLINE':    {'LOGIN': 0.1, 'OFFLINE': 0.9},
        'LOGIN':      {'EXPLORANDO': 1.0},
        'EXPLORANDO': {'EXPLORANDO': 0.2, 'CARRITO': 0.6, 'CHECKOUT': 0.2},
        'CARRITO':    {'CHECKOUT': 0.9, 'ABANDONO': 0.1},
        'CHECKOUT':   {'PAGO': 0.95, 'ABANDONO': 0.05},
        'PAGO':       {'OFFLINE': 1.0},
        'ABANDONO':   {'OFFLINE': 1.0}
    },
    'Abandonador': {
        'OFFLINE':    {'LOGIN': 0.1, 'OFFLINE': 0.9},
        'LOGIN':      {'EXPLORANDO': 1.0},
        'EXPLORANDO': {'EXPLORANDO': 0.3, 'CARRITO': 0.7},
        'CARRITO':    {'ABANDONO': 0.9, 'CHECKOUT': 0.1},
        'CHECKOUT':   {'PAGO': 0.5, 'ABANDONO': 0.5},
        'PAGO':       {'OFFLINE': 1.0},
        'ABANDONO':   {'OFFLINE': 1.0}
    },
    'Cazador de Ofertas': {
        'OFFLINE':    {'LOGIN': 0.1, 'OFFLINE': 0.9},
        'LOGIN':      {'EXPLORANDO': 1.0},
        'EXPLORANDO': {'EXPLORANDO': 0.6, 'CARRITO': 0.1, 'ABANDONO': 0.3},
        'CARRITO':    {'ABANDONO': 0.7, 'CHECKOUT': 0.3},
        'CHECKOUT':   {'PAGO': 0.8, 'ABANDONO': 0.2},
        'PAGO':       {'OFFLINE': 1.0},
        'ABANDONO':   {'OFFLINE': 1.0}
    },
    'Vitrinero': {
        'OFFLINE':    {'LOGIN': 0.1, 'OFFLINE': 0.9},
        'LOGIN':      {'EXPLORANDO': 1.0},
        'EXPLORANDO': {'EXPLORANDO': 0.85, 'ABANDONO': 0.15},
        'CARRITO':    {'ABANDONO': 1.0}, # Nunca llega aquí, pero por seguridad
        'CHECKOUT':   {'ABANDONO': 1.0},
        'PAGO':       {'OFFLINE': 1.0},
        'ABANDONO':   {'OFFLINE': 1.0}
    }
}

ESTADOS_A_TOPICOS = {
    'LOGIN':      ('store.login', 'login'),
    'EXPLORANDO': ('store.ver_producto', 'ver_producto'),
    'CARRITO':    ('store.agregar_carrito', 'agregar_carrito'),
    'CHECKOUT':   ('store.compra', 'compra'),
    'PAGO':       ('store.pago', 'pago'),
    'ABANDONO':   ('store.abandono', 'abandono')
}

CATALOGO = [f"p-{str(i).zfill(4)}" for i in range(1, 100)]
CANALES = ['web', 'mobile', 'iot', 'pos']

class AgenteComprador:
    def __init__(self, agent_id):
        self.agent_id = agent_id
        self.perfil = random.choices(TIPOS_PERFIL, weights=PESOS_PERFIL, k=1)[0]
        self.estado_actual = 'OFFLINE'
        self.session_id = str(uuid.uuid4())
        self.canal = random.choice(CANALES)
        self.lat, self.lon = generar_coordenadas(0)
        self.carrito_actual = []
        self.monto_acumulado = 0.0

    def avanzar_estado(self, tipo_evento=0):
        # 1. COMPORTAMIENTO EXCEPCIONAL POR TEMPORADA ALTA (Flash Sales)
        # Los Cazadores de Ofertas y los Impulsivos reaccionan brutalmente a las ofertas
        if tipo_evento in [1, 2, 3] and self.perfil in ['Cazador de Ofertas', 'Impulsivo']:
            if self.estado_actual in ['EXPLORANDO', 'CARRITO']:
                self.estado_actual = 'CHECKOUT'
                if tipo_evento == 1: producto_flash = 'CYBER-999'
                elif tipo_evento == 2: producto_flash = 'XMAS-999'
                else: producto_flash = 'PERU-28JULIO'
                    
                self.carrito_actual = [producto_flash]
                # Mayor gasto en Navidad
                multiplicador = 1.5 if tipo_evento == 2 else 1.0
                self.monto_acumulado = round(random.uniform(20.0, 100.0) * multiplicador, 2)
                return self._generar_evento()

        # 2. TRANSICIÓN NORMAL DE MARKOV BASADA EN SU AUDIENCIA DIGITAL
        matriz_transicion = TRANSICIONES_PERFIL[self.perfil]
        opciones = list(matriz_transicion[self.estado_actual].keys())
        pesos = list(matriz_transicion[self.estado_actual].values())
        
        nuevo_estado = random.choices(opciones, weights=pesos, k=1)[0]
        self.estado_actual = nuevo_estado
        
        # Reiniciar variables en nuevo login
        if self.estado_actual == 'LOGIN':
            self.session_id = str(uuid.uuid4())
            self.carrito_actual = []
            self.monto_acumulado = 0.0
            self.lat, self.lon = generar_coordenadas(tipo_evento)
            
        if self.estado_actual == 'OFFLINE':
            return None

        return self._generar_evento()

    def _generar_evento(self):
        topico, event_type = ESTADOS_A_TOPICOS[self.estado_actual]
        
        # Late Events (5% de probabilidad)
        ahora = datetime.utcnow()
        if random.random() < 0.05:
            ahora = ahora - timedelta(seconds=random.randint(30, 90))
            
        ts_str = ahora.isoformat() + "Z"
        
        # Inyectamos el perfil en el payload para que Flink/Streamlit puedan usarlo como metadata
        payload = {
            "latitud": self.lat, 
            "longitud": self.lon,
            "perfil_audiencia": self.perfil
        }

        if self.estado_actual == 'EXPLORANDO':
            producto = random.choice(CATALOGO)
            payload["product_id"] = producto
            self.carrito_actual = [producto] 
        elif self.estado_actual == 'CARRITO':
            if not self.carrito_actual:
                self.carrito_actual = [random.choice(CATALOGO)]
            payload["product_id"] = self.carrito_actual[0]
            self.monto_acumulado = round(random.uniform(10.0, 500.0), 2)
        elif self.estado_actual == 'CHECKOUT':
            payload["product_id"] = self.carrito_actual[0] if self.carrito_actual else "desc"
            payload["monto"] = self.monto_acumulado
        elif self.estado_actual == 'PAGO':
            payload["estado"] = "exitoso" if random.random() > 0.1 else "fallido"
            payload["monto"] = self.monto_acumulado

        evento = {
            "event_id": str(uuid.uuid4()),
            "agent_id": self.agent_id,
            "session_id": self.session_id,
            "channel": self.canal,
            "event_type": event_type,
            "ts": ts_str,
            "payload": payload
        }
        return topico, evento

# ==========================================
# 3. TRABAJADORES Y ORQUESTADOR
# ==========================================
def trabajador_simulacion(worker_id, num_agentes, velocidad, bootstrap_servers, flag_evento):
    productor = Producer({'bootstrap.servers': bootstrap_servers})
    agentes = [AgenteComprador(f"w{worker_id}-a{i}") for i in range(num_agentes)]
    
    try:
        while True:
            agente = random.choice(agentes)
            tipo_evento = flag_evento.value
            
            # Ajustar latencia de emisión basado en la época
            if tipo_evento == 4: # Época Baja
                velocidad_efectiva = max(1, velocidad // 5)
            elif tipo_evento in [1, 2, 3]: # Época Alta
                velocidad_efectiva = velocidad * 2
            else: # Normal
                velocidad_efectiva = velocidad
                
            resultado = agente.avanzar_estado(tipo_evento)
            
            if resultado:
                topico, evento_json = resultado
                productor.produce(
                    topico, 
                    key=agente.agent_id.encode('utf-8'), 
                    value=json.dumps(evento_json).encode('utf-8')
                )
                productor.poll(0)
                
            time.sleep(1.0 / velocidad_efectiva)
    except KeyboardInterrupt:
        pass
    finally:
        productor.flush()

def orquestador(num_agentes, num_workers, velocidad, bootstrap_servers):
    agentes_por_worker = num_agentes // num_workers
    flag_evento = Value('i', 0) # 0:Normal, 1:Cyber, 2:Navidad, 3:FiestasPatrias, 4:Baja
    procesos = []

    for i in range(num_workers):
        p = Process(target=trabajador_simulacion, args=(i, agentes_por_worker, velocidad, bootstrap_servers, flag_evento))
        p.start()
        procesos.append(p)

    print("\n=======================================================")
    print("🌍 SIMULADOR AVANZADO: AUDIENCIAS, TEMPORADAS Y GEOGRAFÍA")
    print("=======================================================")
    print("Esperando iniciar ciclos de mercado...\n")

    try:
        while True:
            # 1. Periodo Normal
            tiempo_normal = random.randint(45, 90)
            time.sleep(tiempo_normal)
            
            # 2. Inyectar Temporada Especial (Dura de 30 a 60 segundos)
            nuevo_evento = random.randint(1, 4)
            duracion_evento = random.randint(30, 60)
            hora = datetime.now().strftime('%H:%M:%S')
            
            print("-" * 60)
            if nuevo_evento == 1:
                print(f"🚀 [{hora}] INICIO CYBER MONDAY (Demanda extrema).")
            elif nuevo_evento == 2:
                print(f"🎄 [{hora}] INICIO NAVIDAD (Gasto elevado).")
            elif nuevo_evento == 3:
                print(f"🇵🇪 [{hora}] INICIO FIESTAS PATRIAS (Tráfico centrado en Perú).")
            elif nuevo_evento == 4:
                print(f"📉 [{hora}] INICIO ÉPOCA BAJA (Desaceleración comercial).")
                
            flag_evento.value = nuevo_evento
            time.sleep(duracion_evento)
            
            # 3. Fin del evento
            flag_evento.value = 0
            hora_fin = datetime.now().strftime('%H:%M:%S')
            print(f"⏳ [{hora_fin}] FIN DE TEMPORADA: Retornando a la normalidad de Markov.")
            print("-" * 60 + "\n")
            
    except KeyboardInterrupt:
        print("\nDeteniendo simulación...")
        for p in procesos:
            p.terminate()
            p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--agentes', type=int, default=1000)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--velocidad', type=int, default=50)
    parser.add_argument('--servers', type=str, default='localhost:9092')
    
    args = parser.parse_args()
    orquestador(args.agentes, args.workers, args.velocidad, args.servers)