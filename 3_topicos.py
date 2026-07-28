import json
import paramiko
import os

KEY_FILE = "labsuser.pem"

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

def crear_topicos():
    if not os.path.exists(KEY_FILE) or not os.path.exists('nodos_info.json'):
        print("Error: Faltan archivos clave (labsuser.pem o nodos_info.json).")
        return
        
    with open('nodos_info.json', 'r') as f:
        nodos = json.load(f)

    maestro_ip_pub = nodos['Maestro']['ip_publica']
    
    print(f"\n==========================================")
    print(f" CONFIGURANDO TÓPICOS EN KAFKA KRAFT")
    print(f"==========================================")
    print(f"[Conectando a Maestro - {maestro_ip_pub}]")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(maestro_ip_pub, username='ubuntu', key_filename=KEY_FILE)
        
        # Iterar sobre el diccionario y crear cada tópico
        for topic, (partitions, replication_factor) in TOPICS.items():
            print(f"[{topic}] Creando (Particiones: {partitions}, Replicación: {replication_factor})...")
            
            # Usamos --if-not-exists para evitar errores si el script se corre varias veces
            cmd = f"/home/ubuntu/kafka/bin/kafka-topics.sh --create --topic {topic} --partitions {partitions} --replication-factor {replication_factor} --bootstrap-server localhost:9092 --if-not-exists"
            
            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                print(f"    \u2713 Éxito.")
            else:
                err = stderr.read().decode().strip()
                print(f"    [!] Error: {err}")
                
        print("\n[Hito] Verificando lista de tópicos activos en el clúster:")
        stdin, stdout, stderr = ssh.exec_command("/home/ubuntu/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092")
        
        # Leemos la salida para confirmar que todos están creados
        topics_creados = stdout.read().decode().strip().split('\n')
        for t in topics_creados:
            if t:  # Evita imprimir líneas vacías
                print(f"  - {t}")
            
        ssh.close()
        print("\n==========================================")
        print(" ¡PASO 3 COMPLETADO!")
        print("==========================================")
        
    except Exception as e:
        print(f"Error crítico de conexión: {e}")

if __name__ == '__main__':
    crear_topicos()