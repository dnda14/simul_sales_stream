import json
import paramiko
import os
import sys

KEY_FILE = "labsuser.pem"
SIMULADOR_FILE = "simulador_compradores.py"

def desplegar_simulador():
    # 1. Validaciones previas
    if not os.path.exists(KEY_FILE) or not os.path.exists('nodos_info.json'):
        print("Error: Faltan archivos clave (labsuser.pem o nodos_info.json).")
        return
        
    if not os.path.exists(SIMULADOR_FILE):
        print(f"Error: No se encontró el archivo '{SIMULADOR_FILE}' en el directorio local.")
        print("Por favor, asegúrate de que el código de la simulación esté en esta misma carpeta.")
        return

    with open('nodos_info.json', 'r') as f:
        nodos = json.load(f)

    maestro_ip_pub = nodos['Maestro']['ip_publica']
    
    print("\n==========================================")
    print(" EMPAQUETANDO Y DESPLEGANDO SIMULADOR")
    print("==========================================")
    print(f"[Conectando a Maestro - {maestro_ip_pub}]")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(maestro_ip_pub, username='ubuntu', key_filename=KEY_FILE)
        
        # 2. Subir el archivo mediante SFTP
        print(f"[Hito] Subiendo '{SIMULADOR_FILE}' al Maestro...")
        sftp = ssh.open_sftp()
        sftp.put(SIMULADOR_FILE, f"/home/ubuntu/{SIMULADOR_FILE}")
        sftp.close()
        print("    \u2713 Archivo subido exitosamente.")
        
        # 3. Corregir e instalar dependencias con rutas absolutas infalibles
        print("[Hito] Configurando entorno virtual aislado y corrigiendo dependencias...")
        cmd_dependencias = """
        export DEBIAN_FRONTEND=noninteractive
        
        # Crear un entorno virtual totalmente nuevo
        /usr/bin/python3 -m venv /home/ubuntu/simulador_env
        
        # Actualizar pip usando el ejecutable absoluto del nuevo entorno
        /home/ubuntu/simulador_env/bin/python -m pip install -q --upgrade pip
        
        # Forzar la instalación de dependencias apuntando directamente a este entorno
        /home/ubuntu/simulador_env/bin/python -m pip install -q --force-reinstall numpy faker confluent-kafka
        """
        
        stdin, stdout, stderr = ssh.exec_command(cmd_dependencias)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status == 0:
            print("    \u2713 Dependencias instaladas y enlazadas correctamente.")
        else:
            err = stderr.read().decode().strip()
            print(f"    [!] Error instalando dependencias:\n{err}")
            ssh.close()
            return
            
        ssh.close()
        
        # 4. Instrucciones finales para el usuario
        print("\n==========================================")
        print(" ¡DESPLIEGUE COMPLETADO CON ÉXITO!")
        print("==========================================")
        print("Para iniciar la generación de mensajes, abre una terminal y ejecuta los siguientes pasos:\n")
        
        print("PASO 1: Conéctate al Maestro:")
        print(f"  ssh -i {KEY_FILE} ubuntu@{maestro_ip_pub}\n")
        
        print("PASO 2: Ejecuta la simulación usando la ruta absoluta del entorno (Prueba de 500 agentes):")
        print(f"  /home/ubuntu/simulador_env/bin/python /home/ubuntu/{SIMULADOR_FILE} --agentes 500 --workers 2 --velocidad 100\n")
        
        print("PASO 3 (Opcional): Para ver los eventos en tiempo real, abre OTRA terminal, conéctate al Maestro y ejecuta:")
        print(f"  /home/ubuntu/kafka/bin/kafka-console-consumer.sh --topic store.busqueda --bootstrap-server localhost:9092\n")
        
    except Exception as e:
        print(f"Error crítico durante el despliegue: {e}")

if __name__ == '__main__':
    desplegar_simulador()