# ==========================================================
# PROCESO: EXTRACCIÓN Y CARGA MASIVA DE CATÁLOGO CT
# 
#   - Obtener catálogo de productos (JSON) desde FTP CT
#   - Estandarizar estructura de datos, unificar stock y limpiar SKU
#   - Realizar carga masiva hacia tabla temporal
#
# ENTRADAS:
#   - Servidor FTP CT (productos.json)
#
# SALIDAS:
#   - Tabla temporal: temp_tbl_ct
#
# ==========================================================

import os
import io
import json
import logging
import pandas as pd
from ftplib import FTP
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Inicializar variables de entorno y configuración de logging
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_engine():
    """Establecer conexión a base de datos PostgreSQL usando variables de entorno."""
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", "5432")
        database = os.getenv("DB_NAME")

        # Construir engine con driver psycopg2 requerido para COPY
        return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    except Exception as e:
        logger.error(f"Error en la conexión a la DB: {e}")
        return None

def obtener_catalogo_ct() -> pd.DataFrame:
    """
    Se conecta al servidor FTP, descarga el archivo productos.json, 
    lo convierte en un DataFrame y lo devuelve.
    """
    ftp_host = os.getenv("CT_FTP_HOST")
    ftp_user = os.getenv("CT_FTP_USER")
    ftp_pass = os.getenv("CT_FTP_PASS")
    remote_file_path = "/catalogo_xml/productos.json"

    if not all([ftp_host, ftp_user, ftp_pass]):
        logger.error("Credenciales de FTP CT no encontradas en .env")
        return pd.DataFrame()

    logger.info("Conectando al servidor FTP de CT...")
    try:
        ftp = FTP(ftp_host)
        ftp.login(ftp_user, ftp_pass)

        # Usamos un BytesIO para almacenar el archivo descargado en memoria
        file_content = io.BytesIO()
        
        logger.info(f"Descargando archivo {remote_file_path}...")
        ftp.retrbinary(f"RETR {remote_file_path}", file_content.write)
        ftp.quit()

        file_content.seek(0)
        json_data = json.load(file_content)

        # Normalizar si es una lista o un diccionario
        if isinstance(json_data, list):
            df = pd.json_normalize(json_data, sep='_')
        elif isinstance(json_data, dict):
            key = next(iter(json_data))  # primera clave
            if isinstance(json_data[key], list):
                df = pd.json_normalize(json_data[key], sep='_')
            else:
                df = pd.json_normalize(json_data, sep='_')
        else:
            raise ValueError("El JSON no tiene un formato soportado")

        logger.info(f"Archivo descargado y parseado. Total de registros: {len(df)}")
        return df
    
    except Exception as e:
        logger.error(f"Error al obtener o procesar el archivo desde FTP: {e}")
        return pd.DataFrame()

def procesar_catalogo_ct(df_ct: pd.DataFrame) -> pd.DataFrame:
    """Procesa el catálogo de CT limpiando, normalizando y preparando para carga."""
    if df_ct.empty:
        return df_ct

    logger.info("Procesando estructura del catálogo CT...")

    # Identificar columnas de existencia
    columnas_existencia = [col for col in df_ct.columns if col.startswith("existencia_")]
    
    # Convertir TODAS esas columnas a números (rellenando vacíos con 0)
    for col in columnas_existencia:
        df_ct[col] = pd.to_numeric(df_ct[col], errors='coerce').fillna(0)
    
    # Sumar matemáticamente y forzar a entero (astype(int)) para evitar el error "112.0" en PostgreSQL
    df_ct['Existencia_total'] = df_ct[columnas_existencia].sum(axis=1).astype(int)
    
    # Eliminar las columnas originales de existencia que ya no necesitamos
    df_ct.drop(columns=columnas_existencia, inplace=True)
    
    # Eliminar columnas irrelevantes o no requeridas
    columnas_eliminar = [
        'ean', 'upc', 'idSubCategoria', 'idCategoria', 'activo', 'idMarca', 
        'sustituto', 'idProducto', 'promociones', 'protegido', 'modelo', 
        'subcategoria', 'tipoCambio'
    ]
    df_ct.drop(columns=columnas_eliminar, errors='ignore', inplace=True)
    
    # Renombrar columnas para estandarización
    rename_map = {
        'clave': 'clave_producto_ct',
        'numParte': 'SKU',
        'Existencia_total': 'disponibilidad_ct',
        'descripcion_corta': 'descripcion_ct',
        'nombre': 'nombre_ct',
        'marca': 'marca_ct',
        'categoria': 'categoria_ct',
        'precio': 'precio_ct',
        'moneda': 'moneda_ct',
        'imagen': 'imagen_ct',
        'especificaciones': 'especificaciones_ct'
    }
    df_ct.rename(columns=rename_map, inplace=True)
    
    if 'marca_ct' in df_ct.columns:
        df_ct['marca_ct'] = (
            df_ct['marca_ct']
            .astype(str)
            .str.upper()                              # Todo a mayúsculas
            .str.strip()                              # Sin espacios al inicio/final
            .str.replace(r'\s+', ' ', regex=True)     # Reemplaza múltiples espacios internos por uno solo
        )
    
    
    # Agregar columna de ID del proveedor
    df_ct['ID_PROVEEDOR'] = '3'
    
    # Manejo de valores nulos y limpieza en SKU
    mask_null_sku = df_ct['SKU'].isnull() | (df_ct['SKU'] == '')
    df_ct.loc[mask_null_sku, 'SKU'] = df_ct.loc[mask_null_sku, 'clave_producto_ct']
    
    # Manejar duplicados en SKU
    df_ct.loc[df_ct.duplicated(subset=['SKU'], keep=False), 'SKU'] = df_ct['clave_producto_ct']
    df_ct['SKU'] = df_ct['SKU'].astype(str).str.strip()

    # Eliminar caracteres problemáticos (saltos de línea, tabs) vital para el COPY
    for col in df_ct.select_dtypes(include=['object']).columns:
        df_ct[col] = df_ct[col].astype(str).str.replace(r'[\r\n\t]+', ' ', regex=True).str.strip()
    
    # Serializar estructuras JSON (listas o diccionarios)
    for col in df_ct.columns:
        if df_ct[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_ct[col] = df_ct[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Convertir precio a numérico si es necesario
    if 'precio_ct' in df_ct.columns:
        df_ct['precio_ct'] = pd.to_numeric(df_ct['precio_ct'], errors='coerce').fillna(0)

    return df_ct

def cargar_temporal_ct(df: pd.DataFrame):
    """Realizar carga masiva en tabla temporal usando COPY."""
    engine = get_db_engine()
    if not engine:
        return
    
    try:
        # Convertir DataFrame a CSV en memoria
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='|', na_rep='NULL', quoting=1)
        buffer.seek(0)
        
        # Preparar columnas
        columnas = ', '.join([f'"{c}"' for c in df.columns])
        
        # Sentencia COPY
        copy_sql = f"""
            COPY temp_tbl_ct ({columnas})
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL')
        """
        
        with engine.begin() as conn:
            # Limpiar tabla antes de insertar
            conn.execute(text("TRUNCATE TABLE temp_tbl_ct"))
            logger.info("Tabla temp_tbl_ct truncada. Iniciando flujo de datos masivo...")
            
            # Ejecutar COPY con cursor nativo
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            cursor.copy_expert(sql=copy_sql, file=buffer)
            
            logger.info(f"¡Éxito! {len(df)} registros de CT cargados correctamente.")
            
    except Exception as e:
        logger.error(f"Fallo en carga masiva a PostgreSQL: {e}")
    finally:
        engine.dispose()

# ==========================================================
# Ejecución principal
# ==========================================================
if __name__ == "__main__":
    logger.info("Iniciando rutina de actualización del catálogo CT...")
    
    df_raw = obtener_catalogo_ct()
    
    if not df_raw.empty:
        df_final = procesar_catalogo_ct(df_raw)
        cargar_temporal_ct(df_final)
    else:
        logger.error("No hay datos para procesar. Abortando rutina.")