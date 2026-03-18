# ================================================
# Script para actualizar fichas Icecat en BD
# ================================================
# Descripción:
#   - Obtiene productos sin ficha Icecat desde la base de datos
#   - Consume la API de Icecat en paralelo usando sesiones persistentes
#   - Actualiza la tabla `tbl_producto` usando inserciones en bloque (batch)
#   - Ignora errores de actualización SQL y los registra para revisión
# ================================================

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

# CONFIGURACIÓN
load_dotenv()

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DB ---
def get_db_engine():
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno para la BD")

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        return engine
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None

# ==========================================================
# Función principal para actualizar fichas Icecat
# ==========================================================
def actualizar_fichas_icecat():
    """
    Obtiene productos sin ficha Icecat, consulta la API de Icecat,
    guarda resultados en la base de datos y genera un log de errores.
    """

    # ==========================
    # 1. Obtener productos sin ficha (Optimizado)
    # ==========================
    engine = get_db_engine()
    if engine is None:
        return

    try:
        # Optimización: Solo traer las columnas necesarias
        query = "SELECT csku, cmarca, cnombre, bestatus FROM tbl_producto WHERE cficha_icecat IS NULL;"
        df_informacion = pd.read_sql(query, engine)
        print(f"📦 Catálogo obtenido: {len(df_informacion)} registros pendientes.")
    except Exception as e:
        print("❌ Error al ejecutar la consulta:", e)
        return engine.dispose()

    if df_informacion.empty:
        print("✅ No hay productos pendientes por actualizar.")
        return engine.dispose()

    # ==========================
    # 2. Configuración API Icecat
    # ==========================
    username = os.getenv("ICECAT_USERNAME")
    language = "es"
    app_key = os.getenv("ICECAT_APP_KEY")
    base_url = "https://live.icecat.biz/api"

    resultados, errores = [], []
    contador_exitos = 0

    # Optimización: Uso de Session para mantener vivas las conexiones TCP
    session = requests.Session()

    # ==========================
    # 3. Función de llamada API
    # ==========================
    def hacer_llamada(idx, product_code, brand):
        nonlocal contador_exitos

        if pd.isna(brand) or brand.strip() == "":
            error_dict = {"index": idx, "sku": product_code, "brand": brand, "error": "Marca vacía"}
            errores.append(error_dict)
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
                    data = response.json()
                    print(f"✅ {idx} - {product_code} ({brand}) OK")
                    contador_exitos += 1
                    return {"index": idx, "sku": product_code, "brand": brand, "data": data}
                except ValueError:
                    error_msg = "JSON inválido"
            else:
                error_msg = f"Error HTTP {response.status_code}: {response.text}"

        except requests.RequestException as e:
            error_msg = f"Excepción de red: {e}"

        print(f"❌ {idx} - {product_code} ({brand}) - {error_msg}")
        errores.append({"index": idx, "sku": product_code, "brand": brand, "error": error_msg})
        return None

    # ==========================
    # 4. Ejecutar llamadas en paralelo
    # ==========================
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

    # ==========================
    # 5. Construcción de DataFrame y Solución a KeyError
    # ==========================
    # Solución al error: Se definen las columnas por si 'resultados' está vacío
    df_resultados = pd.DataFrame([
        {
            "sku": r["sku"],
            "marca": r["brand"],
            "jsonrespuesta": (
                json.dumps(r["data"], ensure_ascii=False)
                .replace('\x00', '')
                .replace('\\u0000', '')
            )
        } for r in resultados
    ], columns=["sku", "marca", "jsonrespuesta"])

    if df_resultados.empty:
        print("⚠️ No se obtuvieron resultados válidos de la API. Finalizando proceso.")
        engine.dispose()
        return

    df_informacion = df_informacion.rename(columns={'csku': 'sku'})
    df_merged = pd.merge(df_resultados, df_informacion, how="left", on="sku")

    # ==========================
    # 6. Actualizar base de datos (Optimizado en Batch)
    # ==========================
    updates = list(zip(df_merged['jsonrespuesta'], df_merged['sku']))
    conn = engine.raw_connection()
    cursor = conn.cursor()
    query = """UPDATE tbl_producto SET cficha_icecat = %s WHERE csku = %s;"""
    errores_sql = []

    try:
        # Intenta la carga masiva en bloques (mucho más rápido)
        execute_batch(cursor, query, updates, page_size=500)
        conn.commit()
    except Exception as e_batch:
        conn.rollback()
        print("⚠️ Advertencia: Ocurrió un error en la carga masiva. Aislando errores fila por fila...")
        
        # Fallback: Si el batch falla por un registro corrupto, aplica fila por fila para registrar el error
        for data, sku in updates:
            try:
                cursor.execute(query, (data, sku))
                conn.commit()
            except Exception as e_row:
                conn.rollback()
                print(f"❌ Error al actualizar SKU {sku}: {e_row}")
                errores_sql.append({"sku": sku, "error": str(e_row), "json": data})

    cursor.close()
    conn.close()
    engine.dispose()

    # ==========================
    # 7. Resumen y log de errores
    # ==========================
    print("\n✅ Actualización completa")
    print(f"📊 Total respuestas exitosas: {contador_exitos}")
    print(f"❌ Total errores API: {len(errores)}")
    print(f"⚠️ Errores SQL ignorados/capturados: {len(errores_sql)}")

# ==========================================================
# Ejecución del script
# ==========================================================
if __name__ == "__main__":
    actualizar_fichas_icecat()