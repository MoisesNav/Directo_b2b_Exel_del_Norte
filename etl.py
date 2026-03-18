# ==========================================================
# PROCESO: ETL PRINCIPAL Y CONSOLIDACIÓN B2B
#   - Integrar datos desde tabla temporal hacia modelo productivo
#   - Normalizar información de productos y precios
#   - Insertar nuevos registros y actualizar existentes
#   - Calcular precio ponderado B2B con modelo estadístico
#   - Actualizar tipo de cambio USD
#   - Clasificar productos mediante modelo NLP (Deep Learning)
#   - Mantener consistencia y estatus de catálogo
#
# ENTRADAS:
#   - Tabla temporal: temp_tbl_exel
#   - Tablas productivas: tbl_producto, tbl_detalle_producto
#   - API Banxico (tipo de cambio)
#   - Modelo NLP (TensorFlow)
#
# SALIDAS:
#   - Tablas actualizadas:
#       - tbl_producto
#       - tbl_detalle_producto
#       - tbl_cambio_divisas
#
# PROCESOS CLAVE:
#   - Limpieza y normalización de datos
#   - Separación modelo maestro-detalle
#   - Inserción incremental
#   - Actualización masiva (batch)
#   - Ponderación de precios (distribución gaussiana)
#   - Clasificación automática (Machine Learning)
# ==========================================================

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

# Manipulación de datos
import pandas as pd
import numpy as np

# Base de datos
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, Session
from psycopg2.extras import execute_batch

# Red
import requests

# Inicializar variables de entorno
load_dotenv()

# Configurar logging estructurado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# CONEXIÓN A BASE DE DATOS

def get_db_engine():
    """Establecer pool de conexiones optimizado hacia PostgreSQL."""
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        database = os.getenv("DB_NAME")
        
        # Validar variables críticas
        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno para la BD")

        # Construir URL de conexión
        db_url = URL.create(
            drivername="postgresql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        
        # Crear engine con pool de conexiones
        engine = create_engine(db_url, pool_size=10, max_overflow=20)
        return engine
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None


# UTILIDADES DE NORMALIZACIÓN

def normalize_text(text_val: str) -> str:
    """Normalizar texto eliminando acentos, símbolos y estandarizando formato."""
    if not isinstance(text_val, str):
        return ""
    text_val = text_val.lower()
    text_val = unicodedata.normalize('NFKD', text_val).encode('ASCII', 'ignore').decode('ASCII')
    text_val = re.sub(r'(\d+)\s*(ml|cm|mm|kg|g)', r'\1_\2', text_val)
    text_val = re.sub(r'[^a-z0-9\s\-_\/]', ' ', text_val)
    return text_val.strip()

def normalizar_marca(marca: str) -> str:
    """Estandarizar marca en mayúsculas y sin espacios múltiples."""
    return re.sub(r'\s+', ' ', str(marca).strip()).upper()


# TRANSFORMACIÓN: SEPARACIÓN MAESTRO / DETALLE

def divisora_producto_detalle(df):
    """Separar DataFrame en estructura maestro (producto) y detalle (precios)."""

    # Definir columnas finales del maestro
    columnas_finales_productos = ['csku', 'cnombre', 'cmarca', 'cdescripcion', 'cespecificaciones', 'cimagen', 'bestatus']
    
    # Filtrar columnas existentes
    cols_existentes = [col for col in columnas_finales_productos if col in df.columns]
    df_tbl_productos = df[cols_existentes].copy()

    # Asegurar columnas requeridas
    columnas_requeridas = ["csku", "cnombre", "cmarca", "cdescripcion", "cespecificaciones", "cimagen", "tcreate_at", "tupdate_at", "bestatus"]

    now = datetime.now()
    for col in columnas_requeridas:
        if col not in df_tbl_productos.columns:
            if col in ["tcreate_at", "tupdate_at"]:
                df_tbl_productos[col] = now
            else:
                df_tbl_productos[col] = None

    # Construir detalle de precios por proveedor
    df_precios_filtrado = pd.DataFrame({
        'csku': df['csku'],
        'nid_proveedor': df.get('ID_PROVEEDOR_exel', df.get('ID_PROVEEDOR')), 
        'ndisponibilidad': df.get('disponibilidad_exel'),
        'cmoneda': df.get('moneda_exel'),
        'nprecio': df.get('precio_exel'),
        'cclave_producto': df.get('clave_producto_exel')
    })

    # Filtrar registros inválidos
    df_precios_filtrado = df_precios_filtrado.dropna(subset=['nid_proveedor']).copy()
    df_precios_filtrado = df_precios_filtrado[df_precios_filtrado['csku'].isin(df_tbl_productos['csku'])]

    # Normalizar monedas
    if 'cmoneda' in df_precios_filtrado.columns:
        df_precios_filtrado['cmoneda'] = df_precios_filtrado['cmoneda'].replace({'Pesos': 'MXN', 'Dolares': 'USD'})

    return df_tbl_productos, df_precios_filtrado

# ACTUALIZACIÓN DE ESTATUS

def actualizar_estatus_productos(engine):
    """Actualizar estatus lógico de productos en función de disponibilidad."""
    try:
        with engine.begin() as conn:

            # Desactivar productos sin disponibilidad
            res_desc = conn.execute(text("""
                UPDATE tbl_producto SET bestatus = 'f' 
                WHERE ndisponibilidad_total = 0 
                OR csku NOT IN (SELECT DISTINCT csku FROM tbl_detalle_producto)
                OR ndisponibilidad_total is NULL
            """))

            # Activar productos disponibles
            res_act = conn.execute(text("""
                UPDATE tbl_producto SET bestatus = 't' 
                WHERE ndisponibilidad_total > 0
            """))

            logger.info(f"Estatus actualizado. Desactivados: {res_desc.rowcount}, Activados: {res_act.rowcount}")
    except Exception as e:
        logger.error(f"Error al actualizar estatus: {e}")


# MODELO DE PRECIO PONDERADO (GAUSSIANO)

def ponderacion_de_precio(engine):
    """Calcular precio B2B mediante ponderación gaussiana basada en disponibilidad."""
    try:
        # Obtener tipo de cambio
        df_div = pd.read_sql("SELECT divisa, precio FROM tbl_cambio_divisas", engine)
        tc = dict(zip(df_div['divisa'], df_div['precio']))

        # Obtener precios válidos
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

        # Convertir precios a MXN
        df['precio_mxn'] = df['cmoneda'].map(tc).fillna(1) * df['nprecio']
        resultados = []

        # Agrupar por SKU
        for sku, g in df.groupby('csku'):
            precios = g['precio_mxn'].values
            disponibilidad = g['ndisponibilidad'].values

            # Calcular media y desviación estándar
            mu = precios.mean()
            sigma = precios.std()

            # Ajustar sigma en caso de cero
            if sigma == 0:
                sigma = mu * 0.05

            # Calcular peso gaussiano
            peso_gauss = np.exp(-((precios - mu) ** 2) / (2 * sigma ** 2))
            peso_final = peso_gauss * disponibilidad
            
            # Calcular costo ponderado
            sum_peso = np.sum(peso_final)
            costo = np.sum(precios * peso_final) / sum_peso if sum_peso > 0 else mu

            # Aplicar margen comercial
            costo = float(round(costo * 1.05, 2))

            disponibilidad_total = int(disponibilidad.sum())
            resultados.append((costo, disponibilidad_total, sku))

        # Actualización masiva
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