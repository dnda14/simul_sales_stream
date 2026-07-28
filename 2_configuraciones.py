import json
import paramiko
import time
import os
import concurrent.futures
import uuid
import base64

KEY_FILE = "labsuser.pem"

def execute_steps_on_node(ip, role, steps):
    """
    Se conecta al nodo por SSH y ejecuta una lista de tuplas (Hito, Comando).
    Imprime 'INICIANDO' y 'FINALIZADO' para cada paso.
    Si un paso falla, imprime el error y aborta la configuración de ese nodo.
    """
    print(f"\n[Conectando a {role} - {ip}]")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(ip, username='ubuntu', key_filename=KEY_FILE)
        
        for hito, cmd in steps:
            print(f"[{role}] INICIANDO: {hito}...")
            # Forzamos noninteractive en cada ejecución por ser sesiones separadas
            full_cmd = f"export DEBIAN_FRONTEND=noninteractive; {cmd}"
            
            stdin, stdout, stderr = ssh.exec_command(full_cmd)
            exit_status = stdout.channel.recv_exit_status() 
            
            if exit_status == 0:
                print(f"[{role}] FINALIZADO: {hito} \u2713")
            else:
                err = stderr.read().decode().strip()
                print(f"[{role}] ERROR en '{hito}':\n{err}")
                print(f"[{role}] ABORTANDO configuración del nodo.")
                ssh.close()
                return False
                
        ssh.close()
        return True
    except Exception as e:
        print(f"[{role}] Error crítico de conexión: {e}")
        return False

def generate_kraft_id():
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('utf-8')

def configurar_cluster():
    if not os.path.exists(KEY_FILE) or not os.path.exists('nodos_info.json'):
        print("Error: Faltan archivos clave (labsuser.pem o nodos_info.json).")
        return
        
    with open('nodos_info.json', 'r') as f:
        nodos = json.load(f)

    maestro_ip_pub = nodos['Maestro']['ip_publica']
    maestro_ip_priv = nodos['Maestro']['ip_privada']
    kraft_cluster_id = generate_kraft_id()
    
    # ==========================================
    # PASOS COMUNES (Para todos los nodos)
    # ==========================================
    pasos_comunes = [
        ("Actualizar repositorios e instalar Java", 
         "sudo apt-get update -y -qq && sudo apt-get install -y -qq software-properties-common default-jre wget"),
         
        ("Instalar Python 3.10 y librerías base", 
         "sudo apt-get install -y -qq python3.10 python3.10-venv python3.10-dev"),
         
        ("Descargar y extraer Apache Kafka 3.7.0", 
         """if [ ! -d "/home/ubuntu/kafka" ]; then 
                wget -q https://dlcdn.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz || wget -q https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
                tar -xzf kafka_2.13-3.7.0.tgz && mv kafka_2.13-3.7.0 kafka
            fi"""),
            
        ("Descargar y extraer Apache Flink 1.18.1", 
         """if [ ! -d "/home/ubuntu/flink" ]; then 
                wget -q https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz || wget -q https://archive.apache.org/dist/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
                tar -xzf flink-1.18.1-bin-scala_2.12.tgz && mv flink-1.18.1 flink
            fi"""),
            
        ("Descargar Conector SQL Kafka para Flink", 
         """if [ ! -f "/home/ubuntu/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar" ]; then 
                wget -q -P /home/ubuntu/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
            fi""")
    ]

    # ==========================================
    # PASOS DEL MAESTRO
    # ==========================================
    pasos_maestro = [
        ("Crear entorno virtual Python 3.10 e instalar librerías", 
         "/usr/bin/python3.10 -m venv /home/ubuntu/env && /home/ubuntu/env/bin/pip install -q --upgrade pip setuptools wheel && /home/ubuntu/env/bin/pip install -q apache-flink==1.18.1 faker confluent-kafka"),
         
        ("Generar configuración KRaft (Controller+Broker)", 
         f"""cat <<EOF > /home/ubuntu/kafka/config/kraft/server.properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{maestro_ip_priv}:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://{maestro_ip_priv}:9092
controller.listener.names=CONTROLLER
log.dirs=/tmp/kraft-combined-logs
EOF"""),

        ("Formatear almacenamiento KRaft e iniciar Kafka Maestro", 
         f"""/home/ubuntu/kafka/bin/kafka-storage.sh format -t {kraft_cluster_id} -c /home/ubuntu/kafka/config/kraft/server.properties > /dev/null && 
             /home/ubuntu/kafka/bin/kafka-server-start.sh -daemon /home/ubuntu/kafka/config/kraft/server.properties"""),
             
        ("Generar configuración e iniciar Flink JobManager", 
         f"""cat <<EOF > /home/ubuntu/flink/conf/flink-conf.yaml
jobmanager.rpc.address: {maestro_ip_priv}
jobmanager.bind-host: 0.0.0.0
rest.address: 0.0.0.0
rest.bind-address: 0.0.0.0
jobmanager.memory.process.size: 1600m
EOF
             /home/ubuntu/flink/bin/jobmanager.sh start > /dev/null""")
    ]

    print("\n==========================================")
    print(" DESPLEGANDO NODO MAESTRO")
    print("==========================================")
    exito_maestro = execute_steps_on_node(maestro_ip_pub, "Maestro", pasos_comunes + pasos_maestro)
    
    if not exito_maestro:
        print("\n[!] La configuración del Maestro falló. Se detiene el despliegue del clúster.")
        return

    print("\n[Hito] Esperando 10 segundos para estabilizar el Quorum de Kafka...")
    time.sleep(10)

    # ==========================================
    # PASOS DE LOS TRABAJADORES (WORKERS)
    # ==========================================
    def configurar_worker(idx, worker_name):
        worker_ip_pub = nodos[worker_name]['ip_publica']
        worker_ip_priv = nodos[worker_name]['ip_privada']
        node_id = idx + 2 # IDs para KRaft: 2, 3, 4, 5
        
        pasos_worker = [
            (f"Generar configuración KRaft (Broker {node_id})", 
             f"""cat <<EOF > /home/ubuntu/kafka/config/kraft/server.properties
process.roles=broker
node.id={node_id}
controller.quorum.voters=1@{maestro_ip_priv}:9093
listeners=PLAINTEXT://0.0.0.0:9092
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://{worker_ip_priv}:9092
controller.listener.names=CONTROLLER
log.dirs=/tmp/kraft-broker-logs
EOF"""),

            (f"Formatear almacenamiento KRaft e iniciar Kafka Broker {node_id}", 
             f"""/home/ubuntu/kafka/bin/kafka-storage.sh format -t {kraft_cluster_id} -c /home/ubuntu/kafka/config/kraft/server.properties > /dev/null && 
                 /home/ubuntu/kafka/bin/kafka-server-start.sh -daemon /home/ubuntu/kafka/config/kraft/server.properties"""),
                 
            (f"Generar configuración e iniciar Flink TaskManager {node_id}", 
             f"""cat <<EOF > /home/ubuntu/flink/conf/flink-conf.yaml
jobmanager.rpc.address: {maestro_ip_priv}
taskmanager.bind-host: 0.0.0.0
taskmanager.host: {worker_ip_priv}
taskmanager.memory.process.size: 1728m
EOF
                 /home/ubuntu/flink/bin/taskmanager.sh start > /dev/null""")
        ]
        
        execute_steps_on_node(worker_ip_pub, worker_name, pasos_comunes + pasos_worker)

    print("\n==========================================")
    print(" DESPLEGANDO TRABAJADORES EN PARALELO")
    print("==========================================")
    trabajadores = ['Trabajador1', 'Trabajador2', 'Trabajador3', 'Trabajador4']
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(configurar_worker, idx, worker) for idx, worker in enumerate(trabajadores)]
        concurrent.futures.wait(futures)
        
    print("\n==========================================")
    print(" ¡CONFIGURACIÓN COMPLETADA!")
    print("==========================================")
    print(f"ID del Clúster de Kafka (KRaft): {kraft_cluster_id}")
    print(f"Dashboard de Flink UI disponible en: http://{maestro_ip_pub}:8081")

if __name__ == '__main__':
    configurar_cluster()