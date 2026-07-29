import json
import os
import paramiko

KEY_FILE = "labsuser.pem"
NODOS_FILE = "nodos_info.json"
FLINK_SCRIPT = "flink_procesamiento_total.py"

def desplegar_motor_flink():
    # 1. Validaciones previas locales
    if not os.path.exists(KEY_FILE) or not os.path.exists(NODOS_FILE):
        print(f"Error: No se encontraron los archivos de conexión ({KEY_FILE} o {NODOS_FILE}).")
        return
        
    if not os.path.exists(FLINK_SCRIPT):
        print(f"Error: El archivo '{FLINK_SCRIPT}' no existe en esta carpeta.")
        print("Asegúrate de haberlo creado antes de ejecutar este paso.")
        return

    with open(NODOS_FILE, "r") as f:
        nodos = json.load(f)

    maestro_ip = nodos["Maestro"]["ip_publica"]

    print("\n==========================================")
    print(" PASO 7: DESPLIEGUE DEL MOTOR PYFLINK")
    print("==========================================")
    print(f"[Conectando al Nodo Maestro - {maestro_ip}]")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(maestro_ip, username="ubuntu", key_filename=KEY_FILE)

        # 2. Subir el script a través de SFTP
        print(f"[Hito] Subiendo '{FLINK_SCRIPT}' al Maestro...")
        sftp = ssh.open_sftp()
        ruta_remota = f"/home/ubuntu/{FLINK_SCRIPT}"
        sftp.put(FLINK_SCRIPT, ruta_remota)
        sftp.close()
        ssh.close()
        
        print("  \u2713 Script de procesamiento subido exitosamente.")

        # 3. Instrucciones de ejecución
        print("\n==========================================")
        print(" ¡DESPLIEGUE COMPLETADO CON ÉXITO!")
        print("==========================================")
        print("Tu motor unificado de procesamiento está listo en el clúster.\n")
        print("Para arrancar el Job distribuido, abre tu conexión SSH con el Maestro:")
        print(f"  ssh -i {KEY_FILE} ubuntu@{maestro_ip}\n")
        print("Y ejecuta el siguiente comando (puedes copiarlo y pegarlo tal cual):")
        
        comando_ejecucion = (
            "/home/ubuntu/flink/bin/flink run \\\n"
            "  -pyclientexec /home/ubuntu/simulador_env/bin/python \\\n"
            "  -pyexec /home/ubuntu/simulador_env/bin/python \\\n"
            f"  -py /home/ubuntu/{FLINK_SCRIPT}"
        )
        print("-" * 60)
        print(comando_ejecucion)
        print("-" * 60)
        print("\nNota: Recuerda tener corriendo tu simulador (Paso 5) para que Flink tenga datos que procesar.")

    except Exception as e:
        print(f"Error crítico durante el despliegue SFTP: {e}")

if __name__ == "__main__":
    desplegar_motor_flink()