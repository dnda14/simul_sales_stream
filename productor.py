"""
simulador_compradores.py

Simula miles de agentes-compradores con distintos perfiles, canales de origen
(web, mobile, iot, pos, vehiculo) y estacionalidad, publicando eventos a
topics de Kafka (compatible con Apache Kafka local o Amazon MSK).

Instalación:
    pip install faker numpy confluent-kafka

Uso:
    python simulador_compradores.py --agentes 3000 --workers 3 --velocidad 720

    --velocidad 720 significa que 1 hora simulada = 5 segundos reales (720x).
    Con --workers > 1 se lanzan varios procesos en paralelo (multiprocessing),
    cada uno simulando su propia porción de agentes.

Variables de entorno para apuntar a un cluster real (Kafka local o MSK):
    KAFKA_BOOTSTRAP   -> ej. "b-1.midemo.abc123.kafka.us-east-1.amazonaws.com:9096"
    KAFKA_USER / KAFKA_PASSWORD -> si usas SASL/SCRAM (ver crear_producer)
"""

import argparse
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from multiprocessing import Process

import numpy as np
from faker import Faker
from confluent_kafka import Producer

fake = Faker("es_ES")

# ---------------------------------------------------------------------------
# 1. Catálogo de productos
# ---------------------------------------------------------------------------

CATEGORIAS = {
    "electronica": (100, 3000),
    "ropa": (10, 200),
    "hogar": (15, 500),
    "belleza": (5, 150),
    "deportes": (20, 800),
    "juguetes": (10, 300),
}


def generar_catalogo(n=2000, seed=42):
    rng = np.random.default_rng(seed)
    productos = []
    for i in range(n):
        cat = rng.choice(list(CATEGORIAS.keys()))
        lo, hi = CATEGORIAS[cat]
        productos.append({
            "product_id": f"p{i:05d}",
            "nombre": fake.word().capitalize(),
            "categoria": cat,
            "marca": fake.company(),
            "precio": round(float(rng.uniform(lo, hi)), 2),
        })
    return productos


class Catalogo:
    def __init__(self, productos):
        self.productos = productos
        self.por_categoria = {}
        for p in productos:
            self.por_categoria.setdefault(p["categoria"], []).append(p)
        self.ordenado_precio = sorted(productos, key=lambda p: p["precio"])

    def random(self):
        return random.choice(self.productos)

    def caro(self, percentil=0.85):
        idx = int(len(self.ordenado_precio) * percentil)
        return random.choice(self.ordenado_precio[idx:])


# ---------------------------------------------------------------------------
# 2. Perfiles de agentes
# ---------------------------------------------------------------------------

PERFILES = {
    "compulsivo": dict(
        max_vistos=3, prob_compra_directa=0.55, prob_pasar_a_comparar=0.10,
        prob_conversion=0.75, canal_pref=["mobile", "web", "vehiculo"], usa_caros=False,
    ),
    "comparador": dict(
        max_vistos=12, prob_compra_directa=0.03, prob_pasar_a_comparar=0.6,
        prob_conversion=0.15, canal_pref=["web"], usa_caros=False,
    ),
    "nocturno": dict(
        max_vistos=6, prob_compra_directa=0.2, prob_pasar_a_comparar=0.3,
        prob_conversion=0.3, canal_pref=["mobile", "web"], usa_caros=False,
        solo_horario=(22, 6),
    ),
    "premium": dict(
        max_vistos=4, prob_compra_directa=0.3, prob_pasar_a_comparar=0.2,
        prob_conversion=0.5, canal_pref=["web", "pos"], usa_caros=True,
    ),
    "frecuente": dict(
        max_vistos=5, prob_compra_directa=0.35, prob_pasar_a_comparar=0.25,
        prob_conversion=0.45, canal_pref=["mobile", "iot", "vehiculo"], usa_caros=False,
    ),
    "explorador": dict(
        max_vistos=15, prob_compra_directa=0.0, prob_pasar_a_comparar=0.1,
        prob_conversion=0.0, canal_pref=["web", "mobile"], usa_caros=False,
    ),
    "indeciso": dict(
        max_vistos=6, prob_compra_directa=0.1, prob_pasar_a_comparar=0.3,
        prob_conversion=0.2, canal_pref=["web", "mobile"], usa_caros=False,
        ciclos_carrito=(1, 4),
    ),
    "estacional": dict(
        max_vistos=5, prob_compra_directa=0.1, prob_pasar_a_comparar=0.2,
        prob_conversion=0.15, canal_pref=["web", "mobile", "pos"], usa_caros=False,
    ),
}

PESOS_PERFIL = {  # distribución de la población (deben sumar 1.0)
    "compulsivo": 0.12, "comparador": 0.18, "nocturno": 0.10, "premium": 0.08,
    "frecuente": 0.15, "explorador": 0.15, "indeciso": 0.12, "estacional": 0.10,
}


# ---------------------------------------------------------------------------
# 3. Calendario de eventos (estacionalidad)
# ---------------------------------------------------------------------------

def calendario_eventos(anio=2026):
    # nombre: (inicio, fin, boost_prob_compra, boost_trafico)
    return {
        "campana_escolar": (datetime(anio, 3, 1), datetime(anio, 3, 15), 1.3, 1.8),
        "dia_del_padre":   (datetime(anio, 6, 15), datetime(anio, 6, 21), 1.4, 2.0),
        "fiestas_patrias": (datetime(anio, 7, 25), datetime(anio, 7, 29), 1.5, 2.2),
        "black_friday":    (datetime(anio, 11, 27), datetime(anio, 11, 30), 1.8, 3.5),
        "cyber_monday":    (datetime(anio, 12, 1), datetime(anio, 12, 2), 1.6, 3.0),
        "navidad":         (datetime(anio, 12, 15), datetime(anio, 12, 25), 1.7, 2.8),
    }


def evento_activo(fecha, calendario):
    for nombre, (ini, fin, boost_compra, boost_trafico) in calendario.items():
        if ini <= fecha <= fin:
            return nombre, boost_compra, boost_trafico
    return None, 1.0, 1.0


# ---------------------------------------------------------------------------
# 4. Reloj virtual
# ---------------------------------------------------------------------------

class RelojVirtual:
    """1 segundo real equivale a `velocidad` segundos simulados."""

    def __init__(self, inicio: datetime, velocidad: float):
        self.inicio_real = time.time()
        self.inicio_sim = inicio
        self.velocidad = velocidad

    def ahora(self) -> datetime:
        segundos_reales = time.time() - self.inicio_real
        return self.inicio_sim + timedelta(seconds=segundos_reales * self.velocidad)


# ---------------------------------------------------------------------------
# 5. Agente
# ---------------------------------------------------------------------------

@dataclass
class Agente:
    agent_id: str
    perfil: str
    canal: str
    session_id: str = ""
    carrito: list = field(default_factory=list)

    def en_horario(self, params, fecha: datetime) -> bool:
        if "solo_horario" not in params:
            return True
        ini, fin = params["solo_horario"]
        h = fecha.hour
        return h >= ini or h < fin

    def generar_sesion(self, params, catalogo, boost_compra):
        """Genera la secuencia de eventos de una sesión completa.

        El canal determina el tipo de flujo: pos e iot/vehiculo no
        representan a una persona navegando, así que no tienen
        login/búsqueda/carrito como web y mobile.
        """
        if self.canal == "pos":
            return self._sesion_pos(catalogo)
        if self.canal in ("iot", "vehiculo"):
            return self._sesion_reposicion(catalogo, boost_compra)
        return self._sesion_navegacion(params, catalogo, boost_compra)

    def _sesion_navegacion(self, params, catalogo, boost_compra):
        """Flujo completo para web/mobile: login, navegación, carrito, compra."""
        eventos = []
        self.session_id = str(uuid.uuid4())
        eventos.append(("login", {}))

        vistos = 0
        producto = None
        max_vistos = params["max_vistos"]
        while vistos < max_vistos:
            accion = "busqueda" if random.random() < 0.4 else "ver_producto"
            producto = catalogo.caro() if params["usa_caros"] else catalogo.random()
            eventos.append((accion, {"product_id": producto["product_id"], "precio": producto["precio"]}))
            vistos += 1
            if random.random() < params["prob_compra_directa"] * boost_compra:
                break
            if random.random() > params["prob_pasar_a_comparar"]:
                break

        # ciclo agrega/elimina carrito (perfil indeciso)
        ciclos = params.get("ciclos_carrito")
        if ciclos:
            for _ in range(random.randint(*ciclos)):
                p = catalogo.random()
                eventos.append(("agregar_carrito", {"product_id": p["product_id"], "precio": p["precio"]}))
                eventos.append(("eliminar_carrito", {"product_id": p["product_id"]}))

        prob_conv = min(params["prob_conversion"] * boost_compra, 0.95)
        if producto is not None and random.random() < prob_conv:
            eventos.append(("agregar_carrito", {"product_id": producto["product_id"], "precio": producto["precio"]}))
            eventos.append(("compra", {"product_id": producto["product_id"], "monto": producto["precio"]}))
            exito_pago = random.random() < 0.92
            eventos.append(("pago", {"estado": "exitoso" if exito_pago else "fallido", "monto": producto["precio"]}))
        else:
            eventos.append(("abandono", {}))

        return eventos

    def _sesion_pos(self, catalogo):
        """POS: presencial e inmediato, sin login ni navegación."""
        self.session_id = str(uuid.uuid4())
        p = catalogo.random()
        exito_pago = random.random() < 0.97
        return [
            ("compra", {"product_id": p["product_id"], "monto": p["precio"]}),
            ("pago", {"estado": "exitoso" if exito_pago else "fallido", "monto": p["precio"]}),
        ]

    def _sesion_reposicion(self, catalogo, boost_compra):
        """IoT/vehículo: compra disparada por una señal (sensor/voz), sin navegación humana."""
        self.session_id = str(uuid.uuid4())
        p = catalogo.random()
        if random.random() < min(0.6 * boost_compra, 0.9):
            exito_pago = random.random() < 0.95
            return [
                ("compra", {"product_id": p["product_id"], "monto": p["precio"]}),
                ("pago", {"estado": "exitoso" if exito_pago else "fallido", "monto": p["precio"]}),
            ]
        return []


# ---------------------------------------------------------------------------
# 6. Publicación a Kafka
# ---------------------------------------------------------------------------

def crear_producer():
    config = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    }
    # Descomentar y completar para Amazon MSK con SASL/SCRAM:
    # config.update({
    #     "security.protocol": "SASL_SSL",
    #     "sasl.mechanism": "SCRAM-SHA-512",
    #     "sasl.username": os.environ["KAFKA_USER"],
    #     "sasl.password": os.environ["KAFKA_PASSWORD"],
    # })
    return Producer(config)


def payload_telemetria(canal):
    """El payload de telemetría depende de la fuente: iot y vehiculo miden cosas distintas."""
    if canal == "iot":
        return {"nivel_consumible": random.randint(0, 100), "bateria": random.randint(10, 100)}
    if canal == "vehiculo":
        return {
            "lat": round(random.uniform(-16.5, -16.3), 4),
            "lon": round(random.uniform(-71.6, -71.4), 4),
            "velocidad_kmh": random.randint(0, 90),
            "combustible_pct": random.randint(5, 100),
        }
    return {}


def publicar(producer, canal, agent_id, session_id, event_type, payload, fecha_sim):
    evento = {
        "event_id": str(uuid.uuid4()),
        "agent_id": agent_id,
        "session_id": session_id,
        "channel": canal,
        "event_type": event_type,
        "ts": fecha_sim.isoformat(),
        "payload": payload,
    }
    topic = f"store.{event_type}"
    producer.produce(topic, key=agent_id, value=json.dumps(evento))


# ---------------------------------------------------------------------------
# 7. Loop principal de un worker
# ---------------------------------------------------------------------------

def worker(worker_id, n_agentes, velocidad, anio):
    catalogo = Catalogo(generar_catalogo())
    calendario = calendario_eventos(anio)
    reloj = RelojVirtual(datetime(anio, 1, 1), velocidad)
    producer = crear_producer()

    perfiles = list(PESOS_PERFIL.keys())
    pesos = list(PESOS_PERFIL.values())

    agentes = []
    for i in range(n_agentes):
        perfil = random.choices(perfiles, weights=pesos, k=1)[0]
        canal = random.choice(PERFILES[perfil]["canal_pref"])
        agentes.append(Agente(agent_id=f"w{worker_id}-a{i}", perfil=perfil, canal=canal))

    print(f"[worker {worker_id}] {n_agentes} agentes listos, publicando en {reloj.ahora().date()}...")

    try:
        while True:
            fecha_sim = reloj.ahora()
            _, boost_compra, boost_trafico = evento_activo(fecha_sim, calendario)

            for agente in agentes:
                params = PERFILES[agente.perfil]
                if not agente.en_horario(params, fecha_sim):
                    continue

                if random.random() < (0.02 * boost_trafico):
                    eventos = agente.generar_sesion(params, catalogo, boost_compra)
                    for event_type, payload in eventos:
                        publicar(producer, agente.canal, agente.agent_id, agente.session_id,
                                 event_type, payload, fecha_sim)

                if agente.canal in ("iot", "vehiculo") and random.random() < 0.05:
                    publicar(producer, agente.canal, agente.agent_id, agente.session_id,
                             "telemetria", payload_telemetria(agente.canal), fecha_sim)

            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[worker {worker_id}] deteniendo, enviando eventos pendientes...")
        producer.flush(10)


# ---------------------------------------------------------------------------
# 8. Entry point con multiprocessing
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--agentes", type=int, default=3000, help="agentes totales")
    parser.add_argument("--workers", type=int, default=3, help="procesos en paralelo")
    parser.add_argument("--velocidad", type=float, default=720, help="factor de aceleración del reloj")
    parser.add_argument("--anio", type=int, default=2026)
    args = parser.parse_args()

    agentes_por_worker = args.agentes // args.workers
    procesos = []
    for w in range(args.workers):
        p = Process(target=worker, args=(w, agentes_por_worker, args.velocidad, args.anio))
        p.start()
        procesos.append(p)

    try:
        for p in procesos:
            p.join()
    except KeyboardInterrupt:
        for p in procesos:
            p.terminate()


if __name__ == "__main__":
    main()