import os
import sys
import logging
import threading
import requests
import concurrent.futures
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL, text, bindparam
import time

# CONFIGURACIÓN Y LOGGING 
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DB ---
def get_db_engine():
    try:
        user, password = os.getenv("DB_USER"), os.getenv("DB_PASS")
        host, port = os.getenv("DB_HOST"), os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno para la BD")

        db_url = URL.create(
            drivername="postgresql", username=user, password=password,
            host=host, port=port, database=database
        )
        
        return create_engine(db_url, pool_size=5, max_overflow=10)
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None
    
# ESIONES POR HILO 
thread_local = threading.local()


def procesar_un_producto(producto, engine, endpoint_url):
    """Función 'worker' que será ejecutada por cada hilo de forma independiente."""
    sku = producto['csku']
    
    # Validación de nulos para evitar errores al armar el string
    nombre = producto['cnombre'] or ""
    marca = producto['cmarca'] or "Sin marca"
    categoria = producto['categoria'] or ""
    subcategoria = producto['subcategoria'] or ""
    descripcion = producto['cdescripcion'] or ""
    especificaciones = producto['cespecificaciones'] or ""
    
    # Armado del contexto para el vector
    contenido_vector = (
        f"Producto: {nombre}. "
        f"Marca: {marca}. "
        f"Categoría: {categoria} > {subcategoria}. "
        f"Descripción: {descripcion}. "
        f"Especificaciones: {especificaciones}."
    )

    # Limpiamos espacios extra
    contenido_vector = " ".join(contenido_vector.split())

    # Formato multipart para simular el --form del curl
    payload = {
        'id': (None, sku),
        'contenido': (None, contenido_vector)
    }

    try:
        logger.info(f"Hilo trabajando - Vectorizando SKU: {sku} ...")
        response = requests.post(endpoint_url, files=payload, timeout=20)
        
        if response.ok:
            logger.info(f"✅ Éxito - SKU: {sku} vectorizado.")
            
            # Actualizar bleido = TRUE en la BD
            try:
                # Cada hilo toma una conexión del pool, hace el update y la devuelve
                with engine.begin() as conn_update:
                    update_query = text("UPDATE tbl_producto SET bleido = TRUE WHERE csku = :sku")
                    conn_update.execute(update_query, {"sku": sku})
                logger.info(f"🔄 SKU {sku} marcado como leído.")
            except Exception as db_err:
                logger.error(f"⚠️ Vectorizado, pero error al actualizar BD para SKU {sku}: {db_err}")
        else:
            logger.error(f"❌ Error HTTP {response.status_code} en SKU {sku}: {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de conexión al endpoint en SKU {sku}: {e}")
        
def vectorizar_productos():
    engine = get_db_engine()
    if not engine:
        logger.error("Abortando proceso: No se pudo obtener el engine de la base de datos.")
        return

    endpoint_url = "https://noctua-b2b-dev.azurewebsites.net/agregaProducto/"

    query = text("""
        SELECT
            tp.csku,
            tp.cnombre,
            tp.cmarca,
            tp.cdescripcion,
            tp.cespecificaciones,
            tp.jinfo_general_icecat,
            ts.cnombre_subcategoria AS subcategoria,
            tc.cnombre_categoria AS categoria
        FROM
            tbl_producto AS tp
            INNER JOIN tbl_subcategoria AS ts ON ts.nid = tp.nid_subcategoria
            INNER JOIN tbl_categorias AS tc ON tc.nid = ts.nid_categoria
        WHERE tp.bleido = FALSE
    """)

    logger.info("Ejecutando consulta SQL...")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            productos = [dict(row._mapping) for row in result]
    except Exception as e:
        logger.error(f"Error ejecutando la consulta: {e}")
        return

    total_productos = len(productos)
    if total_productos == 0:
        logger.info("No hay productos pendientes por vectorizar.")
        return

    logger.info(f"Se encontraron {total_productos} productos. Iniciando multihilo...")

    MAX_HILOS = 8
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_HILOS) as executor:
        # Enviamos cada producto a un hilo disponible
        futuros = [
            executor.submit(procesar_un_producto, producto, engine, endpoint_url) 
            for producto in productos
        ]
        
        # Esperamos a que terminen y capturamos excepciones catastróficas de los hilos
        for futuro in concurrent.futures.as_completed(futuros):
            try:
                futuro.result() # Retorna el resultado o lanza la excepción si el hilo falló
            except Exception as e:
                logger.error(f"Error crítico en un hilo de ejecución: {e}")

    logger.info("Proceso de vectorización masiva finalizado.")
    
    
if __name__ == "__main__":
    vectorizar_productos()