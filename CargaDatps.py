import pandas as pd
import mysql.connector
import shutil
import os
import time

def conectar_mysql():
    """ Conecta a la base de datos MySQL. """
    try:
        conn = mysql.connector.connect(
            host='localhost',
            database='ElectroFacil',
            user='root',
            password='nico123'
        )
        if conn.is_connected():
            return conn
    except mysql.connector.Error as e:
        print("Error al conectar a MySQL", e)

def leer_datos_csv(directorio):
    """ Lee todos los archivos CSV en el directorio especificado y los consolida en un DataFrame. """
    datos_consolidados = pd.DataFrame()
    
    for archivo in os.listdir(directorio):
        if archivo.endswith('.csv'):
            ruta_archivo = os.path.join(directorio, archivo)
            print("Leyendo archivo:", archivo)  
            datos = pd.read_csv(ruta_archivo)
            datos['IdLocal'] = int(archivo.split('_')[2].split('.')[0])  
            datos_consolidados = pd.concat([datos_consolidados, datos])
    
    return datos_consolidados

def insertar_datos_mysql(datos, conn):
    """ Inserta los datos en la base de datos MySQL en dos tablas diferentes. """
    cursor = conn.cursor()
    for _, fila in datos.iterrows():
        # Inserción en ventas_consolidadas
        sql_consolidadas = """
        INSERT INTO ventas_consolidadas (IdTransaccion, IdLocal, Fecha, IdCategoria, IdProducto, Producto, Cantidad, PrecioUnitario, TotalVenta)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Fecha=%s, IdCategoria=%s, IdProducto=%s, Producto=%s, Cantidad=%s, PrecioUnitario=%s, TotalVenta=%s
        """
        valores_consolidadas = (
            fila['IdTransaccion'], fila['IdLocal'], fila['Fecha'], fila['IdCategoria'], fila['IdProducto'],
            fila['Producto'], fila['Cantidad'], fila['PrecioUnitario'], fila['TotalVenta'],
            fila['Fecha'], fila['IdCategoria'], fila['IdProducto'], fila['Producto'], fila['Cantidad'], fila['PrecioUnitario'], fila['TotalVenta']
        )
        cursor.execute(sql_consolidadas, valores_consolidadas)

        # Inserción en ventas sin IdLocal
        sql_ventas = """
        INSERT INTO ventas (IdTransaccion, Fecha, IdCategoria, IdProducto, Producto, Cantidad, PrecioUnitario, TotalVenta)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Fecha=%s, IdCategoria=%s, IdProducto=%s, Producto=%s, Cantidad=%s, PrecioUnitario=%s, TotalVenta=%s
        """
        valores_ventas = (
            fila['IdTransaccion'], fila['Fecha'], fila['IdCategoria'], fila['IdProducto'],
            fila['Producto'], fila['Cantidad'], fila['PrecioUnitario'], fila['TotalVenta'],
            fila['Fecha'], fila['IdCategoria'], fila['IdProducto'], fila['Producto'], fila['Cantidad'], fila['PrecioUnitario'], fila['TotalVenta']
        )
        cursor.execute(sql_ventas, valores_ventas)
        
    conn.commit()
    cursor.close()


def mover_archivos(directorio_origen, directorio_destino, archivo):
    ruta_origen = os.path.join(directorio_origen, archivo)
    ruta_destino = os.path.join(directorio_destino, archivo)
    shutil.move(ruta_origen, ruta_destino)
    print(f"Archivo {archivo} movido a {directorio_destino}")


def main():
    directorio_origen = 'C:/Users/nicko/Documents/IntegracionP1/ORIGEN'
    directorio_destino = 'C:/Users/nicko/Documents/IntegracionP1/RESPALDO'

    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino, exist_ok=True)

    while True:
        conn = conectar_mysql()
        if not conn or not conn.is_connected():
            print("No se pudo conectar a la base de datos. Reintentando...")
            time.sleep(10)
            continue

        archivos_originales = set(os.listdir(directorio_origen))
        print("Archivos originales al inicio:", archivos_originales)

        datos_consolidados = leer_datos_csv(directorio_origen)

        if not datos_consolidados.empty:
            print(datos_consolidados.head())
            insertar_datos_mysql(datos_consolidados, conn)

        archivos_despues = set(os.listdir(directorio_origen))
        print("Archivos después de la inserción de datos:", archivos_despues)

        for archivo in archivos_despues:
            if archivo.endswith('.csv'):
                mover_archivos(directorio_origen, directorio_destino, archivo)

        conn.close()
        time.sleep(10)


if __name__ == "__main__":
    main()