# Simul Sales Stream

Sistema de simulación de flujos de compra en tiempo real usando **Apache Kafka** como backbone de mensajería. Genera miles de agentes-compradores con distintos perfiles y canales, publicando eventos a un cluster Kafka desplegado en AWS EC2.

## Stack de Tecnologías y Versiones

| Componente | Versión | Notas |
|---|---|---|
| **Apache Kafka** | **4.3.1** (Scala 2.13) | Modo **KRaft** (sin ZooKeeper) |
| **Python** | 3.11.x | Recomendado por compatibilidad con librerías |
| **confluent-kafka** (Python) | 2.15.0 | Cliente Kafka para el productor |
| **Faker** | 40.36.0 | Generación de datos sintéticos |
| **NumPy** | 2.4.6 | Distribuciones estadísticas |
| **boto3** | (sistema) | Provisión de EC2 en AWS |

## Arquitectura Kafka

- **2 nodos EC2** (`t3.medium`), cada uno con roles **broker + controller**
- Protocolo: **PLAINTEXT** (sin TLS/SASL; protección vía security groups)
- Cluster UUID compartido, generado al momento del despliegue

> **Limitación consciente**: Con solo 2 nodos, el quórum de controlador KRaft no tolera la caída de un nodo (mínimo 3 para tolerancia real). Los datos sí quedan protegidos por replicación = 2. Aceptable para alcance académico.

### Topics

| Topic | Particiones | Replicación |
|---|---|---|
| `store.busqueda` | 6 | 2 |
| `store.ver_producto` | 6 | 2 |
| `store.telemetria` | 6 | 2 |
| `store.login` | 4 | 2 |
| `store.agregar_carrito` | 4 | 2 |
| `store.eliminar_carrito` | 4 | 2 |
| `store.compra` | 3 | 2 |
| `store.pago` | 3 | 2 |
| `store.abandono` | 3 | 2 |

## Compatibilidad con Apache Flink

Kafka 4.3.1 usa el protocolo de clientes **Kafka 4.x**. Para consumir estos topics desde Flink:

| Flink Runtime | Conector recomendado | Maven artifact |
|---|---|---|
| Flink 1.20.x | `flink-connector-kafka` **5.0.0-2.0** o superior | `org.apache.flink:flink-connector-kafka:5.0.0-2.0` |
| Flink 2.0.x+ | `flink-connector-kafka` **5.0.0-2.2** o superior | `org.apache.flink:flink-connector-kafka:5.0.0-2.2` |

> **Importante**: El conector de Flink para Kafka se publica de forma independiente al runtime de Flink. Siempre verifica la versión más reciente en [Maven Central](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka).

### Dependencia Maven (ejemplo para Flink 2.0+)

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>5.0.0-2.2</version>
</dependency>
```

### Dependencia PyFlink (ejemplo)

```bash
pip install apache-flink==2.0.0
# El conector Kafka se agrega como JAR:
# flink-sql-connector-kafka-5.0.0-2.2.jar
```

### Configuración mínima del source Kafka en Flink

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("<ip1>:9092,<ip2>:9092")
    .setTopics("store.compra", "store.pago")
    .setGroupId("flink-consumer-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

## Estructura del Proyecto

```
simul_sales_stream/
├── productor.py              # Simulador de agentes-compradores → Kafka
├── levantar_ec2_kafka.py     # Provisión de 2 EC2 con Kafka KRaft
├── extraer_credenciales.py   # Convierte credenciales.txt → aws_exports.sh
├── requirements.txt          # Dependencias Python (pip)
├── labsuser.pem              # Llave SSH para EC2 (no versionada)
├── credenciales.txt          # Credenciales AWS (no versionada)
└── .gitignore
```

## Quickstart

```bash
# 1. Crear y activar entorno virtual (Python 3.11)
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configurar credenciales AWS
python extraer_credenciales.py
source aws_exports.sh

# 3. Levantar cluster Kafka (2 EC2)
# El script tardará ~5 minutos en aprovisionar todo automáticamente vía SSH
python levantar_ec2_kafka.py

# 4. Ejecutar el productor (Terminal 1)
export KAFKA_BOOTSTRAP="<ip1>:9092,<ip2>:9092"
python productor.py --agentes 3000 --workers 3 --velocidad 720

# 5. Verificar los mensajes en vivo (Terminal 2)
ssh -i labsuser.pem ubuntu@<ip1> '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic store.compra --from-beginning'

# 6. Terminar instancias cuando termines
python levantar_ec2_kafka.py --terminar
```

## Detalles del Despliegue Automático

El script `levantar_ec2_kafka.py` automatiza completamente la creación del cluster KRaft, solucionando el clásico problema de "huevo y gallina" con las IPs privadas en AWS:

1. Lanza 2 instancias EC2 limpias.
2. Obtiene las IPs privadas asignadas dinámicamente por AWS.
3. Se conecta de forma automática vía SSH (`labsuser.pem`) a cada instancia y genera la configuración inyectando ambas IPs en `controller.quorum.voters`.
4. Instala Java, descarga Kafka 4.3.1, formatea el storage de KRaft y lo levanta como servicio `systemd`.
5. El Nodo 1 espera a que el cluster esté sano y automáticamente crea los 9 topics requeridos.

### Verificación del cluster

Si quieres verificar el estado de sincronización de las réplicas directamente en Kafka:

```bash
ssh -i labsuser.pem ubuntu@<ip1> '/opt/kafka/bin/kafka-topics.sh --describe --topic store.compra --bootstrap-server localhost:9092'
```

Deberías ver algo como:
```text
Topic: store.compra  PartitionCount: 3  ReplicationFactor: 2
  Partition: 0  Leader: 2  Replicas: 2,1  Isr: 2,1
  Partition: 1  Leader: 1  Replicas: 1,2  Isr: 1,2
```
- **Isr: 2,1** en todas las particiones confirma que ambos brokers están sincronizados.
- El liderazgo se distribuye dinámicamente entre el nodo 1 y nodo 2.

## Formato de Eventos

Cada mensaje publicado a Kafka tiene esta estructura JSON:

```json
{
  "event_id": "uuid",
  "agent_id": "w0-a123",
  "session_id": "uuid",
  "channel": "web|mobile|iot|pos|vehiculo",
  "event_type": "compra|login|pago|...",
  "ts": "2026-01-15T14:30:00",
  "payload": {
    "product_id": "p00042",
    "monto": 149.99
  }
}
```
