import json
import os
import paramiko

KEY_FILE = "labsuser.pem"
NODOS_FILE = "nodos_info.json"
DASHBOARD_SCRIPT = "dashboard.py"

def desplegar_dashboard():
    # 1. Validaciones locales
    if not os.path.exists(KEY_FILE) or not os.path.exists(NODOS_FILE):
        print(f"Error: No se encontraron los archivos de conexión ({KEY_FILE} o {NODOS_FILE}).")
        return
        
    if not os.path.exists(DASHBOARD_SCRIPT):
        print(f"Error: El archivo '{DASHBOARD_SCRIPT}' no existe en esta carpeta.")
        return

    with open(NODOS_FILE, "r") as f:
        nodos = json.load(f)

    maestro_ip = nodos["Maestro"]["ip_publica"]

    print("\n==========================================")
    print(" PASO 8: DESPLIEGUE DEL DASHBOARD WEB")
    print("==========================================")
    print(f"[Conectando al Nodo Maestro - {maestro_ip}]")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(maestro_ip, username="ubuntu", key_filename=KEY_FILE)

        # 2. Subir el archivo del dashboard actualizado
        print(f"[Hito] Subiendo '{DASHBOARD_SCRIPT}' al Maestro...")
        sftp = ssh.open_sftp()
        sftp.put(DASHBOARD_SCRIPT, f"/home/ubuntu/{DASHBOARD_SCRIPT}")
        sftp.close()
        print("  \u2713 Dashboard subido exitosamente.")

        # 3. Instalar dependencias completas (incluyendo Altair)
        print("[Hito] Instalando Streamlit, Pandas y Altair en el entorno virtual...")
        # CORRECCIÓN AQUÍ: Se añadió 'altair' a la lista de pip install
        cmd_dependencias = (
            "/home/ubuntu/simulador_env/bin/python -m pip install -q streamlit pandas altair"
        )
        stdin, stdout, stderr = ssh.exec_command(cmd_dependencias)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status == 0:
            print("  \u2713 Librerías visuales instaladas correctamente.")
        else:
            err = stderr.read().decode().strip()
            print(f"  [!] Error instalando dependencias:\n{err}")
            ssh.close()
            return

        ssh.close()

        # 4. Instrucciones finales para levantar la web
        print("\n==========================================")
        print(" ¡DESPLIEGUE COMPLETADO CON ÉXITO!")
        print("==========================================")
        print("Para ver tu Dashboard en vivo, abre una terminal SSH hacia el Maestro:\n")
        print(f"  ssh -i {KEY_FILE} ubuntu@{maestro_ip}\n")
        print("Y ejecuta el servidor web de Streamlit apuntando al entorno virtual:")
        print("-" * 75)
        print(f"  /home/ubuntu/simulador_env/bin/streamlit run /home/ubuntu/{DASHBOARD_SCRIPT}")
        print("-" * 75)
        
        print("\n⚠️ IMPORTANTE SOBRE AWS:")
        print("Streamlit se levantará en el puerto 8501 por defecto.")
        print("Asegúrate de que el Security Group de tu Nodo Maestro en AWS tenga")
        print("una regla de entrada (Inbound Rule) permitiendo el tráfico TCP al puerto 8501.")
        print(f"Una vez corriendo, podrás acceder desde tu navegador en: http://{maestro_ip}:8501")

    except Exception as e:
        print(f"Error crítico durante el despliegue: {e}")

if __name__ == "__main__":
    desplegar_dashboard()