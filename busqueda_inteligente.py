import os
import sys
import logging
import threading
import requests
import concurrent.futures
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL, text
import time

# --- CONFIGURACIÓN Y LOGGING ---
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- VARIABLES GLOBALES PARA CONTROL DE FLUJO ---
contador_peticiones = 0
lock_contador = threading.Lock()

# --- CONFIGURACIÓN DB ---
def get_db_engine():
    try:
        user, password = os.getenv("DB_USER"), os.getenv("DB_PASS")
        host, port = os.getenv("DB_HOST"), os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        db_url = URL.create(
            drivername="postgresql", username=user, password=password,
            host=host, port=port, database=database
        )
        return create_engine(db_url, pool_size=5, max_overflow=10)
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None

def procesar_un_producto(producto, engine, endpoint_url):
    """Función 'worker' con control de pausa de 2s por hilo."""
    global contador_peticiones
    sku = producto['csku']
    
    # --- Extracción y Limpieza de Datos ---
    nombre = producto['cnombre'] or ""
    marca = producto['cmarca'] or "Sin marca"
    categoria = producto['categoria'] or ""
    subcategoria = producto['subcategoria'] or ""
    long_desc = producto.get('long_summary_description') or ""
    short_desc = producto.get('short_summary_description') or ""
    
    features = json.dumps(producto.get('features_groups'), ensure_ascii=False) if producto.get('features_groups') else ""
    reasons = json.dumps(producto.get('reasons_to_buy'), ensure_ascii=False) if producto.get('reasons_to_buy') else ""
    
    contenido_vector = (
        f"Producto: {nombre}. Marca: {marca}. Categoría: {categoria} > {subcategoria}. "
        f"Corta: {short_desc}. Larga: {long_desc}. "
        f"Features: {features}. Reasons: {reasons}."
    )
    contenido_vector = " ".join(contenido_vector.split())

    payload = {'id': (None, sku), 'contenido': (None, contenido_vector)}

    try:
        response = requests.post(endpoint_url, files=payload, timeout=20)
        
        if response.ok:
            logger.info(f"✅ SKU: {sku} vectorizado.")
            with engine.begin() as conn_update:
                conn_update.execute(text("UPDATE tbl_producto SET bleido = TRUE WHERE csku = :sku"), {"sku": sku})
        else:
            logger.error(f"❌ Error HTTP {response.status_code} en SKU {sku}")

        # --- LÓGICA DE PAUSA GLOBAL (Opcional tras añadir los 2s) ---
        with lock_contador:
            contador_peticiones += 1
            if contador_peticiones % 100 == 0:
                logger.info(f"⏳ Pausa de control: {contador_peticiones} peticiones alcanzadas. Esperando 5 segundos...")
                time.sleep(5)

    except Exception as e:
        logger.error(f"❌ Error procesando SKU {sku}: {e}")
    
    finally:
        # --- NUEVA PAUSA POR HILO ---
        # Garantiza que, sin importar si hubo éxito o error en la API/BD, 
        # el hilo espere 2 segundos antes de volver al pool a pedir un nuevo producto.
        time.sleep(5)

def vectorizar_productos():
    engine = get_db_engine()
    if not engine: return

    endpoint_url = "https://noctua-b2b-dev.azurewebsites.net/agregaProducto/"

    query = text("""
        SELECT
            tp.csku, tp.cnombre, tp.cmarca,
            tpi.json_icecat -> 'data' -> 'GeneralInfo' -> 'SummaryDescription' ->> 'LongSummaryDescription' AS long_summary_description,
            tpi.json_icecat -> 'data' -> 'GeneralInfo' -> 'SummaryDescription' ->> 'ShortSummaryDescription' AS short_summary_description,
            tpi.json_icecat -> 'data' -> 'FeaturesGroups' AS features_groups,
            tpi.json_icecat -> 'data' -> 'ReasonsToBuy' AS reasons_to_buy,
            ts.cnombre_subcategoria AS subcategoria,
            tc.cnombre_categoria AS categoria
        FROM tbl_producto AS tp
        INNER JOIN tbl_subcategoria AS ts ON ts.nid = tp.nid_subcategoria
        INNER JOIN tbl_categorias AS tc ON tc.nid = ts.nid_categoria
        INNER JOIN tbl_produdcto_icecat AS tpi ON tpi.csku = tp.csku
        WHERE tp.bleido = FALSE
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            productos = [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Error en consulta: {e}")
        engine.dispose()
        return

    if not productos:
        logger.info("No hay productos pendientes.")
        engine.dispose()
        return

    # --- EJECUCIÓN CON 8 HILOS ---
    MAX_HILOS = 3
    logger.info(f"Iniciando con {MAX_HILOS} hilos...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_HILOS) as executor:
        futuros = [executor.submit(procesar_un_producto, p, engine, endpoint_url) for p in productos]
        for futuro in concurrent.futures.as_completed(futuros):
            try:
                futuro.result()
            except Exception as e:
                logger.error(f"Error crítico en hilo: {e}")

    logger.info("Proceso finalizado.")
    
    # Liberar el pool de conexiones al terminar todo el lote
    engine.dispose()

if __name__ == "__main__":
    vectorizar_productos()