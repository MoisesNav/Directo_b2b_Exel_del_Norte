# DIRECTOB2B/ETL/rutina_merge.py
import os
import sys
import time
import json
import pickle
import logging
import gc
import unicodedata
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# MANIPULACIÓN DE DATOS
import pandas as pd
import numpy as np

# BASE DE DATOS
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, Session
from psycopg2.extras import execute_batch

# RED
import requests

# CONFIGURACIÓN
load_dotenv()

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DB ---
def get_db_engine():
    """Crea y retorna un pool de conexiones optimizado y seguro."""
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno para la BD")

        # Seguridad: URL.create escapa caracteres especiales en contraseñas automáticamente
        db_url = URL.create(
            drivername="postgresql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        
        # Optimización: Agregar pool_size para reutilizar conexiones
        engine = create_engine(db_url, pool_size=10, max_overflow=20)
        return engine
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None

# --- UTILIDADES ---
def normalize_text(text_val: str) -> str:
    if not isinstance(text_val, str):
        return ""
    text_val = text_val.lower()
    text_val = unicodedata.normalize('NFKD', text_val).encode('ASCII', 'ignore').decode('ASCII')
    text_val = re.sub(r'(\d+)\s*(ml|cm|mm|kg|g)', r'\1_\2', text_val)
    text_val = re.sub(r'[^a-z0-9\s\-_\/]', ' ', text_val)
    return text_val.strip()

def normalizar_marca(marca: str) -> str:
    return re.sub(r'\s+', ' ', str(marca).strip()).upper()

def divisora_producto_detalle(df):
    columnas_finales_productos = ['csku', 'cnombre', 'cmarca', 'cdescripcion', 'cespecificaciones', 'cimagen', 'bestatus']
    
    cols_existentes = [col for col in columnas_finales_productos if col in df.columns]
    df_tbl_productos = df[cols_existentes].copy()

    columnas_requeridas = ["csku", "cnombre", "cmarca", "cdescripcion", "cespecificaciones", "cimagen", "tcreate_at", "tupdate_at", "bestatus"]

    now = datetime.now()
    for col in columnas_requeridas:
        if col not in df_tbl_productos.columns:
            if col in ["tcreate_at", "tupdate_at"]:
                df_tbl_productos[col] = now
            else:
                df_tbl_productos[col] = None

    df_precios_filtrado = pd.DataFrame({
        'csku': df['csku'],
        'nid_proveedor': df.get('ID_PROVEEDOR_exel', df.get('ID_PROVEEDOR')), 
        'ndisponibilidad': df.get('disponibilidad_exel'),
        'cmoneda': df.get('moneda_exel'),
        'nprecio': df.get('precio_exel'),
        'cclave_producto': df.get('clave_producto_exel')
    })

    df_precios_filtrado = df_precios_filtrado.dropna(subset=['nid_proveedor']).copy()
    df_precios_filtrado = df_precios_filtrado[df_precios_filtrado['csku'].isin(df_tbl_productos['csku'])]

    if 'cmoneda' in df_precios_filtrado.columns:
        df_precios_filtrado['cmoneda'] = df_precios_filtrado['cmoneda'].replace({'Pesos': 'MXN', 'Dolares': 'USD'})

    return df_tbl_productos, df_precios_filtrado
        
def actualizar_estatus_productos(engine):
    try:
        with engine.begin() as conn:
            # Desactivar
            res_desc = conn.execute(text("""
                UPDATE tbl_producto SET bestatus = 'f' 
                WHERE ndisponibilidad_total = 0 OR csku NOT IN (SELECT DISTINCT csku FROM tbl_detalle_producto) or ndisponibilidad_total is NULL
            """))
            # Activar
            res_act = conn.execute(text("""
                UPDATE tbl_producto SET bestatus = 't' 
                WHERE ndisponibilidad_total > 0
            """))
            logger.info(f"Estatus actualizado. Desactivados: {res_desc.rowcount}, Activados: {res_act.rowcount}")
    except Exception as e:
        logger.error(f"Error al actualizar estatus: {e}")
        
def ponderacion_de_precio(engine):
    try:
        df_div = pd.read_sql("SELECT divisa, precio FROM tbl_cambio_divisas", engine)
        tc = dict(zip(df_div['divisa'], df_div['precio']))

        query = """
            SELECT tdp.csku, tdp.cmoneda, tdp.nprecio, tdp.ndisponibilidad
            FROM tbl_detalle_producto tdp
            WHERE tdp.nprecio > 0 
            AND tdp.ndisponibilidad > 0
        """
        df = pd.read_sql(query, engine)
        
        if df.empty:
            logger.warning("No hay precios para ponderar.")
            return

        df['precio_mxn'] = df['cmoneda'].map(tc).fillna(1) * df['nprecio']
        resultados = []

        for sku, g in df.groupby('csku'):
            precios = g['precio_mxn'].values
            disponibilidad = g['ndisponibilidad'].values

            mu = precios.mean()
            sigma = precios.std()

            if sigma == 0:
                sigma = mu * 0.05

            peso_gauss = np.exp(-((precios - mu) ** 2) / (2 * sigma ** 2))
            peso_final = peso_gauss * disponibilidad
            
            # Evitar división por cero en sum(peso_final)
            sum_peso = np.sum(peso_final)
            costo = np.sum(precios * peso_final) / sum_peso if sum_peso > 0 else mu
            costo = float(round(costo * 1.08, 2))
            costo = float(round(costo * 1.16, 2))

            disponibilidad_total = int(disponibilidad.sum())
            resultados.append((costo, disponibilidad_total, sku))

        with engine.begin() as conn:
            raw_conn = conn.connection
            with raw_conn.cursor() as cursor:
                execute_batch(
                    cursor,
                    """
                    UPDATE tbl_producto 
                    SET nprecio_b2b = %s,
                        ndisponibilidad_total = %s
                    WHERE csku = %s
                    """,
                    resultados,
                    page_size=1000
                )
        logger.info(f"Ponderación gaussiana calculada para {len(resultados)} SKUs.")

    except Exception as e:
        logger.error(f"Error en ponderación: {e}")
        
def actualizar_catalogos_db(engine, df_old: pd.DataFrame, df_old_price: pd.DataFrame) -> None:
    try:
        df_prod_clean = df_old.replace({np.nan: None})
        df_price_clean = df_old_price.replace({np.nan: None})

        datos_producto = df_prod_clean.to_dict('records')
        datos_precio = df_price_clean.to_dict('records')

        query_producto = """
            UPDATE tbl_producto
            SET 
                cnombre = %(cnombre)s, cmarca = %(cmarca)s, cdescripcion = %(cdescripcion)s,
                cespecificaciones = %(cespecificaciones)s, cimagen = %(cimagen)s,
                tupdate_at = CURRENT_TIMESTAMP
            WHERE csku = %(csku)s
            AND (
                cnombre IS DISTINCT FROM %(cnombre)s OR cmarca IS DISTINCT FROM %(cmarca)s OR
                cdescripcion IS DISTINCT FROM %(cdescripcion)s OR cespecificaciones IS DISTINCT FROM %(cespecificaciones)s OR
                cimagen IS DISTINCT FROM %(cimagen)s
            );
        """

        query_precio = """
            UPDATE tbl_detalle_producto
            SET 
                ndisponibilidad = %(ndisponibilidad)s, cmoneda = %(cmoneda)s, nprecio = %(nprecio)s
            WHERE csku = %(csku)s;
        """

        logger.info("Iniciando actualización en lote hacia la base de datos...")
        with engine.begin() as conn:
            raw_conn = conn.connection
            with raw_conn.cursor() as cur:
                logger.info(f"Procesando {len(datos_producto)} registros para tbl_producto...")
                execute_batch(cur, query_producto, datos_producto, page_size=1000)
                
                logger.info(f"Procesando {len(datos_precio)} registros para tbl_detalle_producto...")
                execute_batch(cur, query_precio, datos_precio, page_size=1000)
                
        logger.info("¡Actualización completada y confirmada en la base de datos!")
    except Exception as e:
        logger.error(f"Error durante la actualización en lote: {e}")
        raise
        
def actualizar_tipo_cambio_usd(engine):
    token = os.getenv("BANXICO_TOKEN")
    if not token:
        logger.error("Token de Banxico no encontrado en variables de entorno.")
        return

    url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos/oportuno"
    headers = {"Bmx-Token": token}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            dato = float(data['bmx']['series'][0]['datos'][0]['dato'])
            precio = int(dato) + (dato != int(dato)) 
            fecha_actualizacion = datetime.now()

            update_sql = """
            UPDATE tbl_cambio_divisas
            SET precio = :precio, fehca_actualizacion = :fecha_actualizacion
            WHERE divisa = :divisa;
            """
            insert_sql = """
            INSERT INTO tbl_cambio_divisas (divisa, precio, fehca_actualizacion)
            VALUES (:divisa, :precio, :fecha_actualizacion);
            """

            with engine.begin() as conn:
                result = conn.execute(text(update_sql), {
                    "precio": precio, "fecha_actualizacion": fecha_actualizacion, "divisa": "USD"
                })
                if result.rowcount == 0:
                    conn.execute(text(insert_sql), {
                        "divisa": "USD", "precio": precio, "fecha_actualizacion": fecha_actualizacion
                    })
                    logger.info("✅ Registro de divisa insertado.")
                else:
                    logger.info("✅ Registro de divisa actualizado.")
        else:
            logger.error(f"Error Banxico: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error de red o procesamiento con Banxico: {e}")
        
def categorizador_deep_learning(engine):
    logger.info("🧠 Iniciando Categorizador Deep Learning / NLP...")
    
    # Importamos localmente para no cargar TensorFlow en memoria si esta función no se ejecuta
    try:
        import tensorflow as tf
        from tensorflow.keras.utils import pad_sequences
        
        # OPTIMIZACIÓN DE VELOCIDAD/MEMORIA 1: 
        # Filtramos directamente en SQL los que NO tienen subcategoría.
        # Es mucho más eficiente que traer toda la tabla a Pandas y luego filtrar.
        query = """
            SELECT tp.csku, tp.cnombre, tp.cdescripcion, tp.cmarca
            FROM tbl_producto AS tp
            WHERE tp.nid_subcategoria IS NULL;
        """
        df_productos = pd.read_sql(query, engine)
        
        if df_productos.empty:
            logger.info("No hay productos sin subcategoría. Saltando predicción.")
            return

        # Construir rutas
        BASE_DIR = os.path.abspath(os.getcwd())
        path_modelo = os.path.join(BASE_DIR, "Red_neuronal", "modelo_categorias.keras")
        path_tokenizer = os.path.join(BASE_DIR, "Red_neuronal", "tokenizer.pkl")
        path_labelencoder = os.path.join(BASE_DIR, "Red_neuronal", "labelencoder.pkl")
        
        # Cargas
        model = tf.keras.models.load_model(path_modelo)
        with open(path_tokenizer, "rb") as f:
            tokenizer = pickle.load(f)
        with open(path_labelencoder, "rb") as f:
            le = pickle.load(f)
            
        # Preparar texto
        df_productos["texto"] = (
            df_productos["cnombre"].fillna("").astype(str) + " " +
            df_productos["cdescripcion"].fillna("").astype(str) + " " +
            df_productos["cmarca"].fillna("").astype(str)
        )
        df_productos["texto_norm"] = df_productos["texto"].apply(normalize_text)
        
        # NLP
        secuencias = tokenizer.texts_to_sequences(df_productos["texto_norm"])
        X = pad_sequences(secuencias, maxlen=300)
        
        # Predicción
        y_pred = model.predict(X, batch_size=32)
        y_classes = np.argmax(y_pred, axis=1)
        df_productos["categoria_predicha"] = le.inverse_transform(y_classes)
        
        # Cargar subcategorías
        df_subcategorias = pd.read_sql("SELECT nid as id_subcategoria_nueva, cnombre_subcategoria FROM tbl_subcategoria", engine)
        
        # Merge de IDs
        df_productos = df_productos.merge(
            df_subcategorias,
            left_on="categoria_predicha",
            right_on="cnombre_subcategoria",
            how="left"
        )
        
        # Preparar datos para BD
        updates = list(zip(
            df_productos["id_subcategoria_nueva"],
            df_productos["csku"]
        ))
        
        # Limpiar nulos por si el modelo arrojó una clase inexistente
        updates = [u for u in updates if pd.notna(u[0])]
        
        # OPTIMIZACIÓN DE SEGURIDAD Y VELOCIDAD 2:
        # Usamos engine.begin() para que haga auto-commit o rollback si falla
        if updates:
            with engine.begin() as conn:
                raw_conn = conn.connection
                with raw_conn.cursor() as cursor:
                    query_update = """
                        UPDATE tbl_producto
                        SET nid_subcategoria = %s
                        WHERE csku = %s;
                    """
                    execute_batch(cursor, query_update, updates, page_size=1000)
            logger.info(f"¡Éxito! {len(updates)} productos categorizados con Deep Learning.")
            
    except Exception as e:
        logger.error(f"Error en categorizador NLP: {e}")
        
    finally:
        # OPTIMIZACIÓN DE MEMORIA 3: 
        # Forzar destrucción de objetos pesados y limpieza de sesión de Keras
        if 'model' in locals(): del model
        if 'tokenizer' in locals(): del tokenizer
        if 'le' in locals(): del le
        if 'df_productos' in locals(): del df_productos
        
        if 'tf' in locals():
            tf.keras.backend.clear_session()
        gc.collect()

# ==========================================================
# Ejecución Principal Encapsulada (Optimización de Memoria)
# ==========================================================
def main():
    start_time = time.time()
    logger.info("🚀 INICIANDO RUTINA MERGE ETL")
        
    engine = get_db_engine()
    if not engine:
        logger.critical("No se pudo establecer conexión con la DB. Abortando.")
        sys.exit(1)

    tablas_dict = {}
    tablas = ['temp_tbl_exel']

    for t in tablas:
        try:
            tablas_dict[t] = pd.read_sql(f"SELECT * FROM {t}", engine)
            logger.info(f"Cargado {t}: {len(tablas_dict[t])} registros")
        except Exception as e:
            tablas_dict[t] = pd.DataFrame()
            logger.error(f"Error al cargar {t}: {e}")
    
    if tablas_dict['temp_tbl_exel'].empty:
        logger.warning("La tabla temporal está vacía. Finalizando proceso.")
        return

    df_final = tablas_dict['temp_tbl_exel'].copy()
    
    # Liberar memoria de la tabla temporal original
    del tablas_dict['temp_tbl_exel']
    gc.collect()
    
    mapping = {
        'SKU': 'csku', 'nombre_exel': 'cnombre', 'categoria_exel': 'ccategoria',
        'marca_exel': 'cmarca', 'descripcion': 'cdescripcion', 
        'especificaciones_exel': 'cespecificaciones', 'imagen_exel': 'cimagen', 'descripcion_exel':'cdescripcion'
    }
    df_final.rename(columns=mapping, inplace=True)
    
    # Limpieza
    logger.info("Iniciando limpieza de datos...")
    df_final.dropna(subset=['csku', 'cnombre'], inplace=True)
    df_final = df_final[df_final['cnombre'] != 'ND']
    df_final['bestatus'] = 't'
    
    df_final = df_final.replace(['NULL', 'null', 'None', 'nan'], np.nan)
    df_final['cmarca'] = df_final['cmarca'].apply(normalizar_marca)
    df_final = df_final.groupby('csku', as_index=False).first()
    
    # Separar Master / Detalle
    df_prod, df_det = divisora_producto_detalle(df_final)
    
    # Liberar df_final
    del df_final
    gc.collect()
    
    # Obtener Productos Existentes en BD (Optimizado)
    existing_skus_df = pd.read_sql("SELECT csku FROM tbl_producto", engine)
    existing_skus_set = set(existing_skus_df['csku'])
    del existing_skus_df
    
    mask_new = ~df_prod['csku'].isin(existing_skus_set)

    df_new = df_prod[mask_new]
    df_old = df_prod[~mask_new]

    mask_new_price = df_det['csku'].isin(df_new['csku'])
    df_new_price = df_det[mask_new_price]
    df_old_price = df_det[~mask_new_price]
    
    # Insertar nuevos productos
    if not df_new.empty:
        df_new.to_sql('tbl_producto', engine, if_exists='append', index=False, chunksize=1000)
        logger.info(f"Insertados {len(df_new)} nuevos productos.")

    # Insertar precios de productos nuevos
    if not df_new_price.empty:
        df_new_price.to_sql('tbl_detalle_producto', engine, if_exists='append', index=False, chunksize=1000)
        logger.info(f"Insertados {len(df_new_price)} nuevos detalles de precio.")
        
    actualizar_catalogos_db(engine, df_old, df_old_price)
    
    logger.info("Actualizando Precio del Dólar...")
    actualizar_tipo_cambio_usd(engine)
    
    logger.info("Iniciando Ponderación de Precio B2B...")
    ponderacion_de_precio(engine)

    logger.info("Actualizando estatus final de los productos...")
    actualizar_estatus_productos(engine)
    
    logger.info("Iniciando clasificación NLP de productos...")
    categorizador_deep_learning(engine)

    # Cierre explícito del engine
    engine.dispose()
    
    elapsed_time = time.time() - start_time
    logger.info(f"🏁 RUTINA FINALIZADA EXITOSAMENTE en {elapsed_time:.2f} segundos.")

if __name__ == "__main__":
    main()