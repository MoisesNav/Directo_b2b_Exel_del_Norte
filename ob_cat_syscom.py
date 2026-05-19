# ==========================================================
# PROCESO: EXTRACCIÓN Y CARGA MASIVA DE CATÁLOGO SYSCOM
# 
#   - Obtención de token desde Base de Datos
#   - Extracción en "ráfagas controladas" (Rate Limit: 60/min)
#   - Tolerancia a fallos: Reintentos automáticos (Backoff) y Timeouts extendidos
#   - Transformación y limpieza de JSON anidados
#   - Realizar carga masiva hacia tabla temporal (COPY)
# ==========================================================

import os
import json
import time
import ast
import logging
import requests
from requests.exceptions import Timeout, RequestException
import io
import pandas as pd
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
        return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
    except Exception as e:
        logger.error(f"Error en la conexión a la DB: {e}")
        return None

def obtener_token():
    """Obtiene el token de Syscom desde la tabla de proveedores."""
    engine = get_db_engine()
    if not engine:
        return None
    try:
        query = "SELECT ctoken FROM tbl_proveedores WHERE nid_proveedor=2;"
        with engine.connect() as conn:
            resultado = conn.execute(text(query)).fetchone()
            
        if resultado and resultado[0]:
            logger.info("Token de Syscom obtenido correctamente desde la BD.")
            return resultado[0]
        else:
            logger.warning("No se encontró token en la consulta.")
            return None
    except Exception as e:
        logger.error(f"Error al ejecutar la consulta de token: {e}")
        return None
    finally:
        engine.dispose()

def obtener_catalogo_syscom() -> pd.DataFrame:
    """
    Obtiene el catálogo de productos con concurrencia controlada, 
    reintentos automáticos y protección contra timeouts.
    """
    access_token = obtener_token()
    if not access_token:
        return pd.DataFrame()

    productos_url = "https://developers.syscom.mx/api/v1/productos"
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    })

    params_base = {
        'categoria': '32,65747,65811,25,66523,22,30,26,66630,38,27,37',
        'todo': 'false'
    }

    articulos_totales = []

    try:
        # 1. Petición inicial para descubrir el total de páginas
        logger.info("Consultando metadatos iniciales (Página 1)...")
        r_inicial = session.get(productos_url, params={**params_base, 'pagina': 1}, timeout=40)
        r_inicial.raise_for_status()
        data_inicial = r_inicial.json()
        
        total_paginas = int(data_inicial.get("paginas", 1))
        articulos_totales.extend(data_inicial.get("productos", []))
        
        logger.info(f"Total de páginas a procesar: {total_paginas}. Iniciando extracción por lotes...")

        # Función de extracción con Reintentos (Retry Logic)
        def fetch_page(page, max_retries=3):
            for attempt in range(1, max_retries + 1):
                try:
                    # Timeout incrementado a 30s
                    res = session.get(productos_url, params={**params_base, 'pagina': page}, timeout=40)
                    res.raise_for_status()
                    return res.json().get("productos", [])
                except Timeout:
                    wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                    logger.warning(f"Timeout en la página {page} (Intento {attempt}/{max_retries}). Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                except RequestException as e:
                    logger.warning(f"Error de red en la página {page}: {e}. Intento {attempt}/{max_retries}.")
                    time.sleep(2)
            
            logger.error(f"Fallo definitivo en la página {page} tras {max_retries} intentos. Se omitirán esos productos.")
            return []

        tamano_lote = 50 
        
        for inicio_lote in range(2, total_paginas + 1, tamano_lote):
            fin_lote = min(inicio_lote + tamano_lote, total_paginas + 1)
            
            logger.info(f"Procesando lote de páginas: {inicio_lote} a {fin_lote - 1}")
            start_time = time.time()
            
            # Reducimos workers a 5 para no ahogar la base de datos de Syscom
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_page = {executor.submit(fetch_page, p): p for p in range(inicio_lote, fin_lote)}
                for future in as_completed(future_to_page):
                    resultado = future.result()
                    if resultado:
                        articulos_totales.extend(resultado)

            # Control de Tasa (Rate Limiting dinámico)
            tiempo_transcurrido = time.time() - start_time
            if fin_lote <= total_paginas:
                tiempo_espera = 70.0 - tiempo_transcurrido
                if tiempo_espera > 0:
                    logger.info(f"Lote finalizado en {tiempo_transcurrido:.1f}s. Durmiendo {tiempo_espera:.1f}s para evitar error 429...")
                    time.sleep(tiempo_espera)

        # 3. Consolidar DataFrame
        if articulos_totales:
            df_syscom = pd.json_normalize(articulos_totales)
            return df_syscom
        else:
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Fallo crítico en la extracción de Syscom: {e}")
        return pd.DataFrame()
    finally:
        session.close()

def procesar_catalogo_syscom(df: pd.DataFrame) -> pd.DataFrame:
    """Procesa y limpia el catálogo de Syscom eliminando basura y optimizando estructuras."""
    if df.empty:
        return df

    cols_atributos = [col for col in df.columns if "atributos" in col]
    cols_existencia = [col for col in df.columns if col.startswith("existencia.") and col != "existencia.detalle"]
    cols_precios = [col for col in df.columns if col.startswith("precios.") and col != "precios.precio_lista"]
    cols_iconos = [col for col in df.columns if "iconos" in col]
    cols_estaticas = ['imagen_360', 'garantia', 'proyecto', 'sat_key', 'pvol', 'marca_logo', 
                      'link', 'existencia.detalle', 'sat_description',
                      'unidad_de_medida.clave_unidad_sat', 'peso', 'alto', 'largo', 'ancho',
                      'unidad_de_medida.codigo_unidad', 'unidad_de_medida.nombre','categoria_id']
    
    todas_eliminar = set(cols_atributos + cols_existencia + cols_precios + cols_iconos + cols_estaticas)
    df.drop(columns=[c for c in todas_eliminar if c in df.columns], errors='ignore', inplace=True)
    
    df['descripcion'] = df['titulo']
    df['moneda'] = 'USD'
    df['ID_PROVEEDOR'] = '2'

    def extraer_datos_categoria(categorias, key):
        if isinstance(categorias, str) and categorias.startswith("[{"):
            try:
                categorias = ast.literal_eval(categorias)
            except:
                return categorias
        if isinstance(categorias, list):
            return ", ".join(str(cat.get(key, '')) for cat in categorias if isinstance(cat, dict) and key in cat)
        return categorias

    if 'categorias' in df.columns:
        df["categoria"] = df["categorias"].apply(lambda x: extraer_datos_categoria(x, 'nombre'))
        df["categoria_id"] = df["categorias"].apply(lambda x: extraer_datos_categoria(x, 'id'))
        df.drop(columns='categorias', inplace=True)

    rename_map = {
        "producto_id": "clave_producto_syscom",
        "modelo": "SKU",
        "titulo": "nombre_syscom",
        "marca": "marca_syscom",
        "categoria": "categoria_syscom",
        "descripcion": "descripcion_syscom",
        "precios.precio_lista": "precio_syscom",
        "moneda": "moneda_syscom",
        "img_portada": "imagen_syscom",
        "total_existencia": "disponibilidad_syscom",
        'link_privado': 'especificaciones_syscom'
    }
    df.rename(columns=rename_map, inplace=True)

    df['SKU'] = df['SKU'].astype(str).str.strip()
    df.drop_duplicates(subset=['SKU'], keep='first', inplace=True)
    
    if 'marca_syscom' in df.columns:
        df['marca_syscom'] = (
            df['marca_syscom']
            .astype(str)
            .str.upper()                              # Todo a mayúsculas
            .str.strip()                              # Sin espacios al inicio/final
            .str.replace(r'\s+', ' ', regex=True)     # Reemplaza múltiples espacios internos por uno solo
        )
    
    
    df.drop(columns=['categoria_id'], errors='ignore', inplace=True)
    
    df = df[df['nombre_syscom'].notna() & (df['nombre_syscom'] != '')]

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    return df

def cargar_temporal(df: pd.DataFrame):
    """Realiza la carga masiva en tabla temporal usando COPY en memoria."""
    engine = get_db_engine()
    if not engine:
        return
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='|', na_rep='NULL', quoting=1)
        buffer.seek(0)
        
        columnas = ', '.join([f'"{c}"' for c in df.columns])
        copy_sql = f"""
            COPY temp_tbl_syscom ({columnas})
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL')
        """
        
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE temp_tbl_syscom;"))
            logger.info("Tabla truncada. Iniciando flujo de datos masivo...")
            
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            cursor.copy_expert(sql=copy_sql, file=buffer)
            
            logger.info(f"¡Éxito! {len(df)} registros insertados en temp_tbl_syscom.")
            
    except Exception as e:
        logger.error(f"Fallo en carga masiva a BD: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    logger.info("Iniciando rutina de actualización para SYSCOM...")
    
    df_raw = obtener_catalogo_syscom()
    
    if not df_raw.empty:
        logger.info("Procesando catálogo...")
        df_final = procesar_catalogo_syscom(df_raw)

        logger.info("Cargando datos a Base de Datos...")
        cargar_temporal(df_final)
    else:
        logger.error("No se pudo obtener el catálogo de Syscom.")