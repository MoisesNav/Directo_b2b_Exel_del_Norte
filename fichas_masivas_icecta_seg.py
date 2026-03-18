# ==========================================================
# PROCESO: ENRIQUECIMIENTO DE FICHAS TÉCNICAS (ICECAT)
#   - Obtener información técnica enriquecida desde API Icecat
#   - Extraer nodos relevantes del catálogo (estructura controlada)
#   - Minimizar uso de memoria eliminando datos innecesarios
#   - Actualizar productos en base de datos con formato JSONB
#
# ENTRADAS:
#   - Tabla: tbl_producto (productos sin información Icecat)
#   - API Icecat
#
# SALIDAS:
#   - Campos JSONB actualizados en tbl_producto:
#       - jimagen_icecat
#       - jmultimedia_icecat
#       - jinfo_general_icecat
#       - jCatalogObjectCloud
#
# PROCESOS CLAVE:
#   - Consumo API concurrente
#   - Manejo de sesiones HTTP persistentes
#   - Extracción selectiva de nodos JSON
#   - Serialización segura (JSON limpio)
#   - Actualización masiva con fallback por fila
# ==========================================================

import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from psycopg2.extras import execute_batch
import logging
from dotenv import load_dotenv
import os
import sys

# Inicializar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# CONEXIÓN A BASE DE DATOS
def get_db_engine():
    """Establecer conexión directa a PostgreSQL."""
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno para la BD")

        return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None


# UTILIDAD: SERIALIZACIÓN SEGURA JSON

def safe_json_dump(val):
    """Serializar JSON eliminando caracteres nulos incompatibles con PostgreSQL."""
    if val is None:
        return None
    return json.dumps(val, ensure_ascii=False).replace('\x00', '').replace('\\u0000', '')


# FUNCIÓN PRINCIPAL

def actualizar_fichas_icecat():

    # Obtener productos sin información Icecat
    engine = get_db_engine()
    if engine is None:
        return

    try:
        query = """
            SELECT csku, cmarca 
            FROM tbl_producto 
            WHERE jinfo_general_icecat IS NULL;
        """
        df_informacion = pd.read_sql(query, engine)
        print(f"📦 Catálogo obtenido: {len(df_informacion)} registros pendientes.")
    except Exception as e:
        print(" Error al ejecutar la consulta:", e)
        return engine.dispose()

    if df_informacion.empty:
        print(" No hay productos pendientes por actualizar.")
        return engine.dispose()


    # CONFIGURACIÓN API ICECAT

    username = os.getenv("ICECAT_USERNAME")
    language = "es"
    app_key = os.getenv("ICECAT_APP_KEY")
    base_url = "https://live.icecat.biz/api"

    resultados, errores = [], []
    contador_exitos = 0

    # Reutilizar conexión HTTP
    session = requests.Session()


    # FUNCIÓN DE CONSUMO API

    def hacer_llamada(idx, product_code, brand):
        """Consultar API Icecat y extraer nodos relevantes."""
        nonlocal contador_exitos

        # Validar marca
        if pd.isna(brand) or str(brand).strip() == "":
            errores.append({"index": idx, "sku": product_code, "brand": brand, "error": "Marca vacía"})
            return None

        params = {
            "UserName": username,
            "Language": language,
            "ProductCode": product_code,
            "Brand": brand,
            "app_key": app_key
        }

        try:
            response = session.get(base_url, params=params, timeout=10)

            if response.status_code == 200:
                try:
                    full_json = response.json()

                    # Extraer nodo principal
                    data_node = full_json.get("data", {})
                    
                    # Seleccionar únicamente nodos requeridos
                    extracted_data = {
                        "Image": data_node.get("Image"),
                        "Multimedia": data_node.get("Multimedia"),
                        "GeneralInfo": data_node.get("GeneralInfo"),
                        "CatalogObjectCloud": data_node.get("CatalogObjectCloud")
                    }
                    
                    print(f"✅ {idx} - {product_code} ({brand}) OK")
                    contador_exitos += 1
                    return {"sku": product_code, "data": extracted_data}
                except ValueError:
                    error_msg = "JSON inválido"
            else:
                error_msg = f"Error HTTP {response.status_code}: {response.text}"

        except requests.RequestException as e:
            error_msg = f"Excepción de red: {e}"

        print(f"❌ {idx} - {product_code} ({brand}) - {error_msg}")
        errores.append({"index": idx, "sku": product_code, "brand": brand, "error": error_msg})
        return None

    # EJECUCIÓN CONCURRENTE
    tasks = [
        (idx, row['csku'], row['cmarca'])
        for idx, row in df_informacion.iterrows()
        if not pd.isna(row['csku'])
    ]

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_data = {
            executor.submit(hacer_llamada, idx, sku, marca): idx
            for idx, sku, marca in tasks
        }

        for future in as_completed(future_to_data):
            result = future.result()
            if result:
                resultados.append(result)


    # TRANSFORMACIÓN A DATAFRAME
    df_resultados = pd.DataFrame([
        {
            "sku": r["sku"],
            "jimagen": safe_json_dump(r["data"].get("Image")),
            "jmultimedia": safe_json_dump(r["data"].get("Multimedia")),
            "jinfo": safe_json_dump(r["data"].get("GeneralInfo")),
            "jcatalog": safe_json_dump(r["data"].get("CatalogObjectCloud"))
        } for r in resultados
    ], columns=["sku", "jimagen", "jmultimedia", "jinfo", "jcatalog"])

    if df_resultados.empty:
        print("⚠️ No se obtuvieron resultados válidos de la API.")
        engine.dispose()
        return

    # Asegurar consistencia con catálogo original
    df_informacion = df_informacion.rename(columns={'csku': 'sku'})
    df_merged = pd.merge(df_resultados, df_informacion, how="left", on="sku")

    # ACTUALIZACIÓN MASIVA
    updates = list(zip(
        df_merged['jimagen'],
        df_merged['jmultimedia'],
        df_merged['jinfo'],
        df_merged['jcatalog'],
        df_merged['sku']
    ))
    
    conn = engine.raw_connection()
    cursor = conn.cursor()
    
    query = """
        UPDATE tbl_producto 
        SET jimagen_icecat = %s::jsonb,
            jmultimedia_icecat = %s::jsonb,
            jinfo_general_icecat = %s::jsonb,
            "jCatalogObjectCloud" = %s::jsonb
        WHERE csku = %s;
    """

    errores_sql = []

    try:
        # Ejecución batch
        execute_batch(cursor, query, updates, page_size=500)
        conn.commit()
    except Exception:
        # Fallback fila por fila
        conn.rollback()
        print("Advertencia: error en batch, ejecución individual...")
        
        for img, multi, info, cat, sku in updates:
            try:
                cursor.execute(query, (img, multi, info, cat, sku))
                conn.commit()
            except Exception as e_row:
                conn.rollback()
                errores_sql.append({"sku": sku, "error": str(e_row)})

    cursor.close()
    conn.close()
    engine.dispose()

    # Resumen final
    print("\nActualización completa")
    print(f"Total respuestas exitosas: {len(resultados)}")
    print(f"Errores API: {len(errores)}")
    print(f"Errores SQL: {len(errores_sql)}")

# Ejecución
if __name__ == "__main__":
    actualizar_fichas_icecat()