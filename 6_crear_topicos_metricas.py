import json
import os
import paramiko

KEY_FILE = "labsuser.pem"
NODOS_FILE = "nodos_info.json"

# Definición de los tópicos para métricas con sus particiones y replicación
TOPICOS_METRICAS = [
    {"nombre": "dashboard.trafico", "particiones": 3, "replicacion": 2},
    {"nombre": "dashboard.negocio", "particiones": 3, "replicacion": 2},
    {"nombre": "dashboard.audiencias", "particiones": 4, "replicacion": 2},
    {"nombre": "dashboard.alertas", "particiones": 2, "replicacion": 2},
    {"nombre": "dashboard.conversiones", "particiones": 3, "replicacion": 2},
    {"nombre": "dashboard.geo", "particiones": 3, "replicacion": 2},
    # Nuevos tópicos para análisis de embudo y origen
    {"nombre": "dashboard.canales", "particiones": 3, "replicacion": 2},
    {"nombre": "dashboard.eventos", "particiones": 3, "replicacion": 2},
]

def crear_topicos_metricas():
    # 1. Validaciones de archivos locales
    if not os.path.exists(KEY_FILE) or not os.path.exists(NODOS_FILE):
        print(f"Error: No se encontraron los archivos requeridos ({KEY_FILE} o {NODOS_FILE}).")
        return

    with open(NODOS_FILE, "r") as f:
        nodos = json.load(f)

    maestro_ip = nodos["Maestro"]["ip_publica"]

    print("\n==========================================")
    print(" PASO 6: CREACIÓN DE TÓPICOS PARA MÉTRICAS")
    print("==========================================")
    print(f"[Conectando a Nodo Maestro - {maestro_ip}]")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(maestro_ip, username="ubuntu", key_filename=KEY_FILE)

        # 2. Crear cada tópico mediante kafka-topics.sh en el Maestro
        for topico in TOPICOS_METRICAS:
            nombre = topico["nombre"]
            particiones = topico["particiones"]
            replicacion = topico["replicacion"]

            cmd = (
                f"/home/ubuntu/kafka/bin/kafka-topics.sh --create "
                f"--if-not-exists "
                f"--topic {nombre} "
                f"--partitions {particiones} "
                f"--replication-factor {replicacion} "
                f"--bootstrap-server localhost:9092"
            )

            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status == 0:
                print(f"  \u2713 Tópico '{nombre}' verificado/creado ({particiones} particiones, replicación {replicacion}).")
            else:
                err = stderr.read().decode().strip()
                print(f"  [!] Error creando el tópico '{nombre}': {err}")

        ssh.close()
        print("\n==========================================")
        print(" ¡PASO 6 COMPLETADO CON ÉXITO!")
        print("==========================================")

    except Exception as e:
        print(f"Error crítico al conectar por SSH: {e}")

if __name__ == "__main__":
    crear_topicos_metricas()