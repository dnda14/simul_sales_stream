import json
import paramiko
import os

KEY_FILE = "labsuser.pem"

def verificar_topicos():
    if not os.path.exists(KEY_FILE) or not os.path.exists('nodos_info.json'):
        print("Error: Faltan archivos clave (labsuser.pem o nodos_info.json).")
        return
        
    with open('nodos_info.json', 'r') as f:
        nodos = json.load(f)

    maestro_ip_pub = nodos['Maestro']['ip_publica']
    
    print("\n==========================================")
    print(" VERIFICANDO ESTRUCTURA DE TÓPICOS")
    print("==========================================")
    print(f"[Conectando a Maestro - {maestro_ip_pub}]\n")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(maestro_ip_pub, username='ubuntu', key_filename=KEY_FILE)
        
        # Ejecutamos el comando --describe de Kafka
        cmd = "/home/ubuntu/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        # Imprimimos la salida línea por línea
        salida = stdout.read().decode().strip()
        errores = stderr.read().decode().strip()
        
        if salida:
            print(salida)
        
        if errores:
            print(f"\n[!] Hubo advertencias o errores:\n{errores}")
            
        ssh.close()
        print("\n==========================================")
        print(" ¡VERIFICACIÓN COMPLETADA!")
        print("==========================================")
        
    except Exception as e:
        print(f"Error de conexión: {e}")

if __name__ == '__main__':
    verificar_topicos()