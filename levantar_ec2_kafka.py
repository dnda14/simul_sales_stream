"""
levantar_ec2_kafka.py

Levanta 2 instancias EC2 con Apache Kafka 4.3.1 en modo KRaft (sin ZooKeeper).
Cada nodo combina los roles broker + controller (node.id 1 y 2).

Estrategia de despliegue:
  1. Lanza 2 EC2 sin user_data (para obtener las IPs privadas)
  2. Genera scripts de setup con las IPs ya conocidas
  3. Configura cada nodo vía SCP + SSH (evita el problema de cloud-init)

Limitación consciente: con 2 nodos el quórum de controlador no tolera la caída
de un nodo (se necesitarían 3 mínimo), pero los datos quedan protegidos por
replicación = 2.  Alcance académico aceptable.

Topics creados (compatibles con productor.py → topic = f"store.{event_type}"):
  6 particiones / repl 2 → store.busqueda, store.ver_producto, store.telemetria
  4 particiones / repl 2 → store.login, store.agregar_carrito, store.eliminar_carrito
  3 particiones / repl 2 → store.compra, store.pago, store.abandono

Uso:
    # Cargar credenciales AWS
    source aws_exports.sh

    # Crear las 2 EC2 + Kafka
    python levantar_ec2_kafka.py

    # Terminar ambas EC2
    python levantar_ec2_kafka.py --terminar
"""

import base64
import boto3
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid as _uuid

# ─── Configuración ────────────────────────────────────────────────────────────
REGION = "us-east-1"
KEY_NAME = "vockey"
INSTANCE_TYPE = "t3.medium"
AMI_ID = "ami-0c7217cdde317cfec"  # Ubuntu 22.04 LTS us-east-1
KAFKA_VERSION = "4.3.1"
SCALA_VERSION = "2.13"
NUM_BROKERS = 2
KAFKA_PORT = 9092
CONTROLLER_PORT = 9093
EC2_INFO_FILE = "ec2_kafka_info.json"
PEM_FILE = "labsuser.pem"

# Topics: {nombre: (particiones, replicación)}
TOPICS = {
    "store.busqueda":         (6, 2),
    "store.ver_producto":     (6, 2),
    "store.telemetria":       (6, 2),
    "store.login":            (4, 2),
    "store.agregar_carrito":  (4, 2),
    "store.eliminar_carrito": (4, 2),
    "store.compra":           (3, 2),
    "store.pago":             (3, 2),
    "store.abandono":         (3, 2),
}

# Opciones SSH comunes (no preguntar por host key, sin timeout largo)
SSH_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "ConnectTimeout=10",
    "-o", "LogLevel=ERROR",
]


# ─── Security Group ──────────────────────────────────────────────────────────
def crear_security_group(ec2):
    """Crea (o reutiliza) un SG con los puertos necesarios para Kafka KRaft."""
    sg_name = "kafka-kraft-cluster-sg"
    try:
        print("🔒 Creando Security Group...")
        resp = ec2.create_security_group(
            GroupName=sg_name,
            Description="Kafka KRaft cluster: SSH + broker + controller + inter-broker",
        )
        sg_id = resp["GroupId"]

        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                # SSH desde cualquier lugar (lab académico)
                {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                 "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
                # Kafka broker (productor se conecta aquí)
                {"IpProtocol": "tcp", "FromPort": KAFKA_PORT, "ToPort": KAFKA_PORT,
                 "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
                # KRaft controller (comunicación inter-nodo)
                {"IpProtocol": "tcp", "FromPort": CONTROLLER_PORT, "ToPort": CONTROLLER_PORT,
                 "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
            ],
        )
        print(f"   SG creado: {sg_id}")
    except ec2.exceptions.ClientError as e:
        if "InvalidGroup.Duplicate" in str(e):
            sgs = ec2.describe_security_groups(GroupNames=[sg_name])
            sg_id = sgs["SecurityGroups"][0]["GroupId"]
            print(f"   SG existente reutilizado: {sg_id}")
        else:
            raise
    return sg_id


# ─── Script de setup por nodo ─────────────────────────────────────────────────
def generar_script_setup(node_id, cluster_uuid, private_ips, crear_topics=False):
    """Genera el script bash de setup para un nodo Kafka.

    Args:
        node_id:       1 o 2 (único por broker)
        cluster_uuid:  UUID compartido por todo el cluster
        private_ips:   lista de IPs privadas [ip_nodo1, ip_nodo2]
        crear_topics:  si True, este nodo crea los topics tras arrancar
    """
    kafka_tgz = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}.tgz"
    kafka_dir = f"kafka_{SCALA_VERSION}-{KAFKA_VERSION}"
    kafka_url = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{kafka_tgz}"

    # Quórum de controladores: node_id@ip_privada:9093
    controller_quorum = ",".join(
        f"{i+1}@{ip}:{CONTROLLER_PORT}" for i, ip in enumerate(private_ips)
    )

    # Bloque de creación de topics (solo para el nodo designado)
    topics_block = ""
    if crear_topics:
        topics_cmds = "\n".join(
            f"/opt/kafka/bin/kafka-topics.sh --create --if-not-exists "
            f"--bootstrap-server localhost:{KAFKA_PORT} "
            f"--topic {topic} --partitions {parts} --replication-factor {repl}"
            for topic, (parts, repl) in TOPICS.items()
        )
        topics_block = f"""
# ── 7. Esperar a que el cluster esté listo y crear topics ────────────────
echo "Esperando 60s a que ambos brokers se sincronicen..."
sleep 60

for i in $(seq 1 20); do
    if /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:{KAFKA_PORT} 2>/dev/null; then
        echo "Broker respondiendo, creando topics..."
        break
    fi
    echo "Intento $i/20 — esperando 10s..."
    sleep 10
done

{topics_cmds}

echo "✅ Topics creados (o ya existían):"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:{KAFKA_PORT}
"""

    return f"""#!/bin/bash
set -euxo pipefail

echo "═══ Configurando Kafka nodo {node_id} ═══"

# ── 1. Instalar Java si no está ──────────────────────────────────────────
if ! command -v java &>/dev/null; then
    apt-get update -y
    apt-get install -y openjdk-17-jre-headless wget
else
    echo "Java ya instalado: $(java -version 2>&1 | head -1)"
fi

# ── 2. Descargar y extraer Kafka ─────────────────────────────────────────
cd /opt
if [ ! -d kafka ]; then
    wget -q {kafka_url}
    tar -xzf {kafka_tgz}
    mv {kafka_dir} kafka
    rm -f {kafka_tgz}
fi

# ── 3. Obtener IP pública de esta instancia ──────────────────────────────
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \\
       -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \\
           http://169.254.169.254/latest/meta-data/public-ipv4)
echo "IP pública: $PUBLIC_IP"

# ── 4. server.properties (KRaft, broker + controller) ───────────────────
mkdir -p /opt/kafka-data

cat <<'PROPEOF' > /opt/kafka/config/server.properties
# ─── KRaft multi-nodo (2 brokers, sin ZooKeeper) ─────────────────────────
process.roles=broker,controller
node.id={node_id}
controller.quorum.voters={controller_quorum}

# Listeners
listeners=PLAINTEXT://0.0.0.0:{KAFKA_PORT},CONTROLLER://0.0.0.0:{CONTROLLER_PORT}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# Directorios
log.dirs=/opt/kafka-data

# Replicación y particiones por defecto
num.partitions=3
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1

# Retención
log.retention.hours=168
log.segment.bytes=1073741824
PROPEOF

# Insertar advertised.listeners con la IP pública real
sed -i "/^listeners=/a advertised.listeners=PLAINTEXT://$PUBLIC_IP:{KAFKA_PORT}" \\
    /opt/kafka/config/server.properties

# ── 5. Formatear storage KRaft (limpiar si ya existía) ──────────────────
rm -rf /opt/kafka-data/*
/opt/kafka/bin/kafka-storage.sh format \\
    -t {cluster_uuid} \\
    -c /opt/kafka/config/server.properties

# ── 6. Servicio systemd ─────────────────────────────────────────────────
cat <<SVCEOF > /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka {KAFKA_VERSION} (KRaft)
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

echo "✅ Kafka nodo {node_id} arrancado."
{topics_block}
echo "═══ Setup completo para nodo {node_id} ═══"
"""


# ─── Ejecución remota vía SSH ─────────────────────────────────────────────────
def esperar_ssh(ip, max_intentos=30, intervalo=10):
    """Espera hasta que el servidor SSH esté listo en la EC2."""
    print(f"   Esperando SSH en {ip}...", end="", flush=True)
    for i in range(max_intentos):
        result = subprocess.run(
            ["ssh", "-i", PEM_FILE] + SSH_OPTS +
            [f"ubuntu@{ip}", "echo ok"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            print(" ✓")
            return True
        print(".", end="", flush=True)
        time.sleep(intervalo)
    print(" ✗")
    raise TimeoutError(f"SSH no disponible en {ip} después de {max_intentos * intervalo}s")


def ejecutar_remoto(ip, script_contenido):
    """Copia un script al nodo y lo ejecuta vía SSH."""
    # Guardar script en archivo temporal local
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
        f.write(script_contenido)
        tmp_path = f.name

    try:
        # SCP: copiar al nodo
        subprocess.run(
            ["scp", "-i", PEM_FILE] + SSH_OPTS +
            [tmp_path, f"ubuntu@{ip}:/tmp/kafka_setup.sh"],
            check=True, capture_output=True,
        )

        # SSH: ejecutar con sudo (output en tiempo real)
        process = subprocess.Popen(
            ["ssh", "-i", PEM_FILE] + SSH_OPTS +
            [f"ubuntu@{ip}", "sudo bash /tmp/kafka_setup.sh"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )

        # Imprimir output en tiempo real
        for line in process.stdout:
            print(f"   [{ip}] {line}", end="")
        process.wait()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, "ssh")

    finally:
        os.unlink(tmp_path)


# ─── Lanzar cluster ──────────────────────────────────────────────────────────
def lanzar_cluster():
    ec2 = boto3.client("ec2", region_name=REGION)

    # Verificar que el PEM existe
    if not os.path.exists(PEM_FILE):
        print(f"❌ No se encontró {PEM_FILE} en el directorio actual.")
        sys.exit(1)

    # Asegurar permisos del PEM (SSH lo requiere)
    os.chmod(PEM_FILE, 0o400)

    sg_id = crear_security_group(ec2)

    # ── Paso 1: Lanzar 2 instancias (sin user_data)
    print(f"\n🚀 Lanzando {NUM_BROKERS} instancias EC2 ({INSTANCE_TYPE})...")
    response = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        MinCount=NUM_BROKERS,
        MaxCount=NUM_BROKERS,
        SecurityGroupIds=[sg_id],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": "kafka-kraft-broker"}],
        }],
    )

    instance_ids = [inst["InstanceId"] for inst in response["Instances"]]
    print(f"   Instancias creadas: {instance_ids}")

    # ── Paso 2: Esperar running y obtener IPs
    print("⏳ Esperando que las instancias estén running...")
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=instance_ids)

    desc = ec2.describe_instances(InstanceIds=instance_ids)
    nodos = []
    for reservation in desc["Reservations"]:
        for inst in reservation["Instances"]:
            nodos.append({
                "instance_id": inst["InstanceId"],
                "public_ip": inst.get("PublicIpAddress", "N/A"),
                "private_ip": inst.get("PrivateIpAddress", "N/A"),
            })

    # Ordenar para asignación consistente de node_id
    nodos.sort(key=lambda n: n["instance_id"])
    private_ips = [n["private_ip"] for n in nodos]

    # Renombrar nodos con su node_id
    for idx, nodo in enumerate(nodos):
        node_id = idx + 1
        ec2.create_tags(
            Resources=[nodo["instance_id"]],
            Tags=[{"Key": "Name", "Value": f"kafka-kraft-broker-{node_id}"}],
        )

    # ── Paso 3: Generar cluster UUID
    print("🔑 Generando Cluster UUID...")
    raw = _uuid.uuid4().bytes
    cluster_uuid = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    print(f"   Cluster UUID: {cluster_uuid}")

    # ── Paso 4: Esperar SSH y configurar cada nodo
    print("\n⚙️  Configurando Kafka en cada nodo vía SSH...")

    # Esperar SSH en ambos nodos
    for nodo in nodos:
        esperar_ssh(nodo["public_ip"])

    # Configurar nodo 2 primero (sin topics), luego nodo 1 (con topics)
    # Así el nodo 2 ya está arrancado cuando el nodo 1 intenta crear topics
    print(f"\n📦 Configurando Nodo 2 ({nodos[1]['public_ip']})...")
    script_nodo2 = generar_script_setup(2, cluster_uuid, private_ips, crear_topics=False)
    ejecutar_remoto(nodos[1]["public_ip"], script_nodo2)

    print(f"\n📦 Configurando Nodo 1 ({nodos[0]['public_ip']})...")
    script_nodo1 = generar_script_setup(1, cluster_uuid, private_ips, crear_topics=True)
    ejecutar_remoto(nodos[0]["public_ip"], script_nodo1)

    # ── Paso 5: Resumen
    bootstrap_servers = ",".join(f'{n["public_ip"]}:{KAFKA_PORT}' for n in nodos)

    print("\n" + "=" * 70)
    print("  CLUSTER KAFKA KRAFT — 2 BROKERS")
    print("=" * 70)
    for idx, nodo in enumerate(nodos):
        node_id = idx + 1
        print(f"\n  Nodo {node_id}:")
        print(f"    Instance ID:  {nodo['instance_id']}")
        print(f"    IP Pública:   {nodo['public_ip']}")
        print(f"    IP Privada:   {nodo['private_ip']}")
        print(f"    SSH:          ssh -i {PEM_FILE} ubuntu@{nodo['public_ip']}")

    print(f"\n  Cluster UUID:       {cluster_uuid}")
    print(f"  Bootstrap Servers:  {bootstrap_servers}")
    print(f"\n  Para tu productor:")
    print(f"    export KAFKA_BOOTSTRAP=\"{bootstrap_servers}\"")
    print(f"    python productor.py --agentes 10 --workers 1 --velocidad 720")
    print("=" * 70)

    # ── Guardar info
    info = {
        "cluster_uuid": cluster_uuid,
        "bootstrap_servers": bootstrap_servers,
        "nodos": nodos,
        "topics": {t: {"partitions": p, "replication": r} for t, (p, r) in TOPICS.items()},
    }
    with open(EC2_INFO_FILE, "w") as f:
        json.dump(info, f, indent=4)
    print(f"\n📄 Info del cluster guardada en {EC2_INFO_FILE}")


# ─── Terminar cluster ────────────────────────────────────────────────────────
def terminar_cluster():
    """Termina todas las instancias del cluster y limpia el archivo de info."""
    try:
        with open(EC2_INFO_FILE, "r") as f:
            info = json.load(f)
    except FileNotFoundError:
        print(f"❌ No se encontró {EC2_INFO_FILE}. ¿Creaste el cluster con este script?")
        sys.exit(1)

    ec2 = boto3.client("ec2", region_name=REGION)
    instance_ids = [n["instance_id"] for n in info["nodos"]]

    print(f"🗑️  Terminando {len(instance_ids)} instancias: {instance_ids}")
    ec2.terminate_instances(InstanceIds=instance_ids)
    print("✅ Señal de terminación enviada. Las instancias se apagarán pronto.")

    if os.path.exists(EC2_INFO_FILE):
        os.remove(EC2_INFO_FILE)
        print(f"   {EC2_INFO_FILE} eliminado.")


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--terminar":
        terminar_cluster()
    else:
        lanzar_cluster()