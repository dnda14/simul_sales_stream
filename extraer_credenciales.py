archivo_entrada = "credenciales.txt"
archivo_salida = "aws_exports.sh"

try:
    with open(archivo_entrada, "r") as f_in, open(archivo_salida, "w") as f_out:
        for linea in f_in:
            linea = linea.strip()
            
            if "=" in linea:
                clave, valor = linea.split("=", 1)
                
                clave = clave.strip().upper()
                valor = valor.strip().strip('"').strip("'")
                
                f_out.write(f'export {clave}="{valor}"\n')
                
    print(f"✅ Archivo '{archivo_salida}' generado con éxito.")

except FileNotFoundError:
    print("❌ Error: No se encontró el archivo 'credenciales'.")