import json
import time
import uuid
import random
import argparse
from datetime import datetime, timedelta
from multiprocessing import Process, Value
from confluent_kafka import Producer
from faker import Faker

fake = Faker('es_ES')

# ==========================================
# 1. DEFINICIÓN DE LA MÁQUINA DE ESTADOS
# ==========================================
# Probabilidades de transición de un estado a otro (Cadena de Markov)
TRANSICIONES = {
    'OFFLINE':    {'LOGIN': 0.1, 'OFFLINE': 0.9},
    'LOGIN':      {'EXPLORANDO': 1.0},
    'EXPLORANDO': {'EXPLORANDO': 0.6, 'CARRITO': 0.3, 'ABANDONO': 0.1},
    'CARRITO':    {'EXPLORANDO': 0.2, 'CHECKOUT': 0.6, 'ABANDONO': 0.2},
    'CHECKOUT':   {'PAGO': 0.8, 'ABANDONO': 0.2},
    'PAGO':       {'OFFLINE': 1.0},
    'ABANDONO':   {'OFFLINE': 1.0}
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
        self.estado_actual = 'OFFLINE'
        self.session_id = str(uuid.uuid4())
        self.canal = random.choice(CANALES)
        # 4. ENRIQUECIMIENTO GEOESPACIAL: Coordenadas fijas por sesión
        self.lat = float(fake.latitude())
        self.lon = float(fake.longitude())
        self.carrito_actual = []
        self.monto_acumulado = 0.0

    def avanzar_estado(self, flash_sale_activa=False):
        # 2. INYECCIÓN DE CAOS (Data Skew)
        # Si hay venta relámpago, los usuarios en la tienda saltan directo a comprar el producto en oferta
        if flash_sale_activa and self.estado_actual in ['EXPLORANDO', 'CARRITO']:
            self.estado_actual = 'CHECKOUT'
            self.carrito_actual = ['FLASH-999'] # Producto altamente concurrido
            self.monto_acumulado = 19.99
            return self._generar_evento()

        # Transición normal de Markov
        opciones = list(TRANSICIONES[self.estado_actual].keys())
        pesos = list(TRANSICIONES[self.estado_actual].values())
        nuevo_estado = random.choices(opciones, weights=pesos, k=1)[0]
        
        self.estado_actual = nuevo_estado
        
        # Reiniciar sesión si vuelve a entrar
        if self.estado_actual == 'LOGIN':
            self.session_id = str(uuid.uuid4())
            self.carrito_actual = []
            self.monto_acumulado = 0.0
            
        if self.estado_actual == 'OFFLINE':
            return None

        return self._generar_evento()

    def _generar_evento(self):
        topico, event_type = ESTADOS_A_TOPICOS[self.estado_actual]
        
        # 3. EVENTOS DESORDENADOS (Late Events)
        # 5% de probabilidad de que el evento llegue con 30-90 segundos de retraso
        ahora = datetime.utcnow()
        if random.random() < 0.05:
            retraso = random.randint(30, 90)
            ahora = ahora - timedelta(seconds=retraso)
        
        ts_str = ahora.isoformat() + "Z"
        
        payload = {
            "latitud": self.lat,
            "longitud": self.lon
        }

        # Llenar payload según el estado
        if self.estado_actual == 'EXPLORANDO':
            producto = random.choice(CATALOGO)
            payload["product_id"] = producto
            self.carrito_actual = [producto] # Memoria temporal
            
        elif self.estado_actual == 'CARRITO':
            if not self.carrito_actual:
                self.carrito_actual = [random.choice(CATALOGO)]
            payload["product_id"] = self.carrito_actual[0]
            self.monto_acumulado = round(random.uniform(10.0, 500.0), 2)
            
        elif self.estado_actual == 'CHECKOUT':
            payload["product_id"] = self.carrito_actual[0] if self.carrito_actual else "desc"
            payload["monto"] = self.monto_acumulado
            
        elif self.estado_actual == 'PAGO':
            # 10% de probabilidad de pago fallido
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

def trabajador_simulacion(worker_id, num_agentes, velocidad, bootstrap_servers, flag_flash_sale):
    productor = Producer({'bootstrap.servers': bootstrap_servers})
    
    # Inicializar agentes de este worker
    agentes = [AgenteComprador(f"w{worker_id}-a{i}") for i in range(num_agentes)]
    
    print(f"[Worker {worker_id}] Iniciado con {num_agentes} agentes independientes.")
    
    try:
        while True:
            # Seleccionar un agente aleatorio para que actúe
            agente = random.choice(agentes)
            
            # Revisar si estamos en evento de caos global
            en_flash_sale = flag_flash_sale.value == 1
            
            resultado = agente.avanzar_estado(flash_sale_activa=en_flash_sale)
            
            if resultado:
                topico, evento_json = resultado
                productor.produce(
                    topico, 
                    key=agente.agent_id.encode('utf-8'), 
                    value=json.dumps(evento_json).encode('utf-8')
                )
                productor.poll(0) # Liberar buffer asíncrono
                
            time.sleep(1.0 / velocidad)
            
    except KeyboardInterrupt:
        pass
    finally:
        productor.flush()

def orquestador(num_agentes, num_workers, velocidad, bootstrap_servers):
    agentes_por_worker = num_agentes // num_workers
    flag_flash_sale = Value('i', 0) # Variable compartida entre procesos
    procesos = []

    for i in range(num_workers):
        p = Process(target=trabajador_simulacion, args=(i, agentes_por_worker, velocidad, bootstrap_servers, flag_flash_sale))
        p.start()
        procesos.append(p)

    print("\n🚀 SIMULADOR AVANZADO INICIADO 🚀")
    print("--------------------------------------------------")
    print(f"Agentes Totales: {num_agentes} | Workers: {num_workers} | Tasa base: {velocidad} evt/s")
    print("Mecánicas activas: Máquina de Estados, Data Skew, Late Events, Geoespacial")
    print("Presiona Ctrl+C para detener.\n")

    try:
        while True:
            # Bucle del orquestador: Decide aleatoriamente si lanza un evento de caos
            time.sleep(random.randint(15, 45))
            if random.random() < 0.3: # 30% de probabilidad cada ciclo
                print("\n⚡ [CAOS INYECTADO] ¡FLASH SALE INICIADA! Todos los agentes comprando FLASH-999 ⚡")
                flag_flash_sale.value = 1
                time.sleep(5) # La oferta dura 5 segundos
                flag_flash_sale.value = 0
                print("⏳ [CAOS TERMINADO] Flash sale finalizada. Retornando a Markov.\n")
    except KeyboardInterrupt:
        print("\nDeteniendo simulación...")
        for p in procesos:
            p.terminate()
            p.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Simulador de E-Commerce Avanzado (Markov & Caos)")
    parser.add_argument('--agentes', type=int, default=1000, help='Total de compradores virtuales')
    parser.add_argument('--workers', type=int, default=2, help='Hilos de procesamiento paralelos')
    parser.add_argument('--velocidad', type=int, default=50, help='Eventos por segundo por worker')
    parser.add_argument('--servers', type=str, default='localhost:9092', help='Kafka Bootstrap Servers')
    
    args = parser.parse_args()
    orquestador(args.agentes, args.workers, args.velocidad, args.servers)