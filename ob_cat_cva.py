# ==========================================================
# PROCESO: EXTRACCIÓN Y CARGA MASIVA DE CATÁLOGO CVA
# 
#   - Autenticación en API CVA (Login)
#   - Extracción concurrente de páginas del catálogo
#   - Integración de información técnica con concurrencia
#   - Estandarización y limpieza de estructura de datos
#   - Realizar carga masiva hacia tabla temporal (COPY)
#
# ENTRADAS:
#   - API CVA (Catálogo e información técnica)
#
# SALIDAS:
#   - Tabla temporal: temp_tbl_cva
# ==========================================================

import json
import os
import logging
import requests
import io
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")

        # Construir engine con driver psycopg2 requerido para COPY
        return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    except Exception as e:
        logger.error(f"Error en la conexión a la DB: {e}")
        return None

def obtener_catalogo_cva(cache_dir="cache_tecnica", usar_cache=True, max_workers=20) -> pd.DataFrame:
    """
    Obtiene el catálogo de clientes de CVA optimizado con concurrencia y pool de conexiones HTTP.
    """
    user_cva = os.getenv("CVA_USER")
    pass_cva = os.getenv("CVA_PASS")

    if not user_cva or not pass_cva:
        logger.error("Credenciales de CVA (CVA_USER, CVA_PASS) no encontradas en .env")
        return pd.DataFrame()

    login_url = "https://apicvaservices.grupocva.com/api/v2/user/login"
    catalogo_url = "https://apicvaservices.grupocva.com/api/v2/catalogo_clientes/lista_precios?pdf=true"
    
    # Usar Session para connection pooling (mejora rendimiento de red)
    session = requests.Session()
    
    try:
        # Paso 1: Login y obtención del token
        logger.info("Autenticando en CVA...")
        auth_data = {"user": user_cva, "password": pass_cva}
        response = session.post(login_url, json=auth_data)
        response.raise_for_status()
        
        token = response.json().get("token")
        if not token:
            raise ValueError("No se pudo obtener el token de autorización de CVA.")
        
        session.headers.update({"Authorization": f"Bearer {token}"})

        # Paso 2: Obtener catálogo completo con concurrencia
        logger.info("Descargando páginas del catálogo CVA de forma concurrente...")
        articulos_totales = []
        paginas_fallidas = []

        def fetch_page(page):
            try:
                url = f"{catalogo_url}&page={page}"
                res = session.get(url, timeout=15)
                res.raise_for_status()
                data = res.json()
                return data.get("articulos", [])
            except Exception as e:
                logger.warning(f"Error en página {page}: {e}")
                return []

        # Rango estimado de páginas (puedes ajustar o hacerlo dinámico si la API lo permite)
        total_pages = 390
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {executor.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
            for future in as_completed(future_to_page):
                resultado = future.result()
                if resultado:
                    articulos_totales.extend(resultado)
                else:
                    paginas_fallidas.append(future_to_page[future])

        if not articulos_totales:
            logger.error("No se extrajeron artículos del catálogo base.")
            return pd.DataFrame()

        # Generar DataFrame una sola vez al final (Eficiencia de Memoria)
        df_cva = pd.json_normalize(articulos_totales)
        df_cva["ficha_tecnica_pdf"] = df_cva.get("pdf", None)

        # Paso 3: Obtener info técnica con concurrencia
        logger.info("Obteniendo información técnica de los productos...")
        
        def obtener_info_tecnica(clave):
            # Lógica de caché local
            if usar_cache:
                os.makedirs(cache_dir, exist_ok=True)
                filepath = os.path.join(cache_dir, f"{clave}.json")
                if os.path.exists(filepath):
                    with open(filepath, "r", encoding="utf-8") as f:
                        return f.read()

            info_url = f"https://apicvaservices.grupocva.com/api/v2/catalogo_clientes/informacion_tecnica?clave={clave}"
            try:
                r = session.get(info_url, timeout=10)
                r.raise_for_status()
                data = json.dumps(r.json(), ensure_ascii=False)
                
                if usar_cache:
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(data)
                return data
            except:
                return None

        claves_unicas = df_cva["clave"].dropna().unique()
        info_tecnica_dict = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_clave = {executor.submit(obtener_info_tecnica, clave): clave for clave in claves_unicas}
            for future in as_completed(future_to_clave):
                clave = future_to_clave[future]
                info_tecnica_dict[clave] = future.result()

        df_cva["info_tecnica"] = df_cva["clave"].map(info_tecnica_dict)
        return df_cva

    except Exception as e:
        logger.error(f"Fallo crítico en la extracción de CVA: {e}")
        return pd.DataFrame()
    finally:
        session.close()

def procesar_catalogo_cva(df: pd.DataFrame) -> pd.DataFrame:
    """Estandarizar, limpiar y preparar DataFrame de CVA para carga."""
    if df.empty:
        return df

    # Eliminar columnas innecesarias
    columnas_eliminar = ['promociones', 'id', 'grupo', 'brand_image', 'disponible', 'garantia', 'clase', 'ficha_tecnica_pdf', 'requiere_serie']
    df.drop(columns=columnas_eliminar, errors='ignore', inplace=True)
    
    # Crear nueva columna 'nombre' basada en 'descripcion'
    if 'descripcion' in df.columns:
        df['nombre'] = df['descripcion']
    
    # Agregar identificador del proveedor
    df['ID_PROVEEDOR'] = '1'
    
    # Renombrar columnas para estandarizar nombres
    rename_map = {
        'principal': 'categoria_cva',
        'disponibleCD': 'disponibilidad_cva',
        'codigo_fabricante': 'SKU',
        'clave': 'clave_producto_cva',
        'nombre': 'nombre_cva',
        'descripcion': 'descripcion_cva',
        'marca': 'marca_cva',
        'precio': 'precio_cva',
        'moneda': 'moneda_cva',
        'imagen': 'imagen_cva',
        'clave_proveedor': 'clave_producto_cva',
        'info_tecnica': 'especificaciones_cva'
    }
    df.rename(columns=rename_map, inplace=True)
    
    if 'marca_cva' in df.columns:
        df['marca_cva'] = (
            df['marca_cva']
            .astype(str)
            .str.upper()                              # Todo a mayúsculas
            .str.strip()                              # Sin espacios al inicio/final
            .str.replace(r'\s+', ' ', regex=True)     # Reemplaza múltiples espacios internos por uno solo
        )
    
    # Limpieza de SKU y manejo de duplicados
    df['SKU'] = df['SKU'].astype(str).str.strip()
    df.loc[df.duplicated(subset=['SKU'], keep=False), 'SKU'] = df['clave_producto_cva']
    
    # Filtro determinista (Reemplaza el borrado de índice hardcodeado 2016)
    # df = df[df['SKU'] != 'SKU_PROBLEMATICO_SI_LO_HAY'] 
    
    df['moneda_cva'] = df['moneda_cva'].replace({'Pesos': 'MXN', 'Dolares': 'USD'})
        
    # Filtrar productos sin precio o sin descuento válido
    df = df[~(df['precio_cva'].isin(['Precio no disponible', 'Sin Descuento']))]
    
    # Limpiar cadenas de texto (saltos de línea, tabs)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.replace(r'[\r\n\t]+', ' ', regex=True).str.strip()
        # Normalizar NaN string generado por la limpieza
        df[col] = df[col].replace('nan', np.nan)

    # Serializar estructuras JSON (Listas o Dicts)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    return df

def cargar_temporal(df: pd.DataFrame):
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
            COPY temp_tbl_cva ({columnas})
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL')
        """
        
        with engine.begin() as conn:
            # Limpiar tabla antes de insertar (TRUNCATE es más rápido que DELETE y libera espacio)
            conn.execute(text("TRUNCATE TABLE temp_tbl_cva"))
            logger.info("Tabla temp_tbl_cva truncada. Iniciando flujo de datos masivo...")
            
            # Ejecutar COPY con cursor nativo
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            cursor.copy_expert(sql=copy_sql, file=buffer)
            
            logger.info(f"¡Éxito! {len(df)} registros cargados correctamente.")
            
    except Exception as e:
        logger.error(f"Fallo en carga masiva de CVA: {e}")
    finally:
        engine.dispose()

def limpiar_cache(cache_dir="cache_tecnica"):
    """Limpia los archivos JSON cacheados temporalmente."""
    if os.path.exists(cache_dir):
        try:
            for archivo in os.listdir(cache_dir):
                ruta_archivo = os.path.join(cache_dir, archivo)
                if os.path.isfile(ruta_archivo):
                    os.remove(ruta_archivo)
            logger.info(f"Directorio de caché '{cache_dir}' limpiado con éxito.")
        except Exception as e:
            logger.error(f"Error limpiando caché: {e}")

# Ejecución principal
if __name__ == "__main__":
    logger.info("Iniciando rutina de actualización para CVA...")
    
    df_raw = obtener_catalogo_cva()
    
    if not df_raw.empty:
        logger.info("Procesando catálogo...")
        df_final = procesar_catalogo_cva(df_raw)
        
        logger.info("Cargando catálogo procesado a BD...")
        cargar_temporal(df_final)
        
        limpiar_cache()
    else:
        logger.error("No hay datos válidos para procesar.")