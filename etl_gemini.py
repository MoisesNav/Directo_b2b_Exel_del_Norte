# DIRECTOB2B/ETL/rutina_merge_all.py
"""
ETL Unificado — Todos los mayoristas: CT, Exel, CVA, Syscom
Flujo:
  1.  Cargar tablas temporales desde BD
  2.  Limpiar y normalizar cada proveedor
  3.  Consolidar master (tbl_producto) con prioridad configurable
  4.  Armar detalle (tbl_detalle_producto) apilando proveedores
  5.  Separar nuevos SKUs vs existentes
  6.  INSERT nuevos productos + detalles
  7.  UPDATE existentes (solo si cambio algo en texto)
  8.  UPSERT detalles existentes por (csku, nid_proveedor)
  9.  Normalizar marcas en tbl_producto (JOIN contra tbl_normalizacion_marcas)
  10. Actualizar tipo de cambio USD (Banxico)
  11. Ponderar precio B2B gaussiano
  12. Sincronizar bestatus activo / inactivo
  13. Categorizar sin subcategoria con Deep Learning
  14. Eliminar tablas temporales
Para agregar un mayorista nuevo: solo anade su bloque en PROVEEDORES.
"""
import gc
import logging
import os
import pickle
import re
import sys
import time
import unicodedata
from datetime import datetime
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURACION CENTRAL DE PROVEEDORES
# ==============================================================================
PROVEEDORES: dict = {
    "ct": {
        "tabla_temp": "temp_tbl_ct",
        "cols_producto": {
            "nombre_ct":           "cnombre",
            "marca_ct":            "cmarca",
            "categoria_ct":        "ccategoria",
            "descripcion_ct":      "cdescripcion",
            "especificaciones_ct": "cespecificaciones",
            "imagen_ct":           "cimagen",
        },
        "cols_detalle": {
            "disponibilidad_ct":   "ndisponibilidad",
            "precio_ct":           "nprecio",
            "moneda_ct":           "cmoneda",
            "clave_producto_ct":   "cclave_producto",
        },
        "moneda_map": {},
    },
    "exel": {
        "tabla_temp": "temp_tbl_exel",
        "cols_producto": {
            "nombre_exel":           "cnombre",
            "marca_exel":            "cmarca",
            "categoria_exel":        "ccategoria",
            "descripcion_exel":      "cdescripcion",
            "especificaciones_exel": "cespecificaciones",
            "imagen_exel":           "cimagen",
        },
        "cols_detalle": {
            "disponibilidad_exel":   "ndisponibilidad",
            "precio_exel":           "nprecio",
            "moneda_exel":           "cmoneda",
            "clave_producto_exel":   "cclave_producto",
        },
        "moneda_map": {"Pesos": "MXN", "Dolares": "USD"},
    },
    "cva": {
        "tabla_temp": "temp_tbl_cva",
        "cols_producto": {
            "nombre_cva":           "cnombre",
            "marca_cva":            "cmarca",
            "categoria_cva":        "ccategoria",
            "descripcion_cva":      "cdescripcion",
            "especificaciones_cva": "cespecificaciones",
            "imagen_cva":           "cimagen",
        },
        "cols_detalle": {
            "disponibilidad_cva":   "ndisponibilidad",
            "precio_cva":           "nprecio",
            "moneda_cva":           "cmoneda",
            "clave_producto_cva":   "cclave_producto",
        },
        "moneda_map": {"Pesos": "MXN", "Dolares": "USD"},
    },
    "syscom": {
        "tabla_temp": "temp_tbl_syscom",
        "cols_producto": {
            "nombre_syscom":           "cnombre",
            "marca_syscom":            "cmarca",
            "categoria_syscom":        "ccategoria",
            "descripcion_syscom":      "cdescripcion",
            "especificaciones_syscom": "cespecificaciones",
            "imagen_syscom":           "cimagen",
        },
        "cols_detalle": {
            "disponibilidad_syscom":   "ndisponibilidad",
            "precio_syscom":           "nprecio",
            "moneda_syscom":           "cmoneda",
            "clave_producto_syscom":   "cclave_producto",
        },
        "moneda_map": {"Pesos": "MXN", "Dolares": "USD"},
    },
}

# Prioridad general para consolidar campos de texto
PRIORIDAD: list = ["exel", "syscom", "cva", "ct"]
# Especificaciones: syscom al final (entrega formato JSON-like)
PRIORIDAD_ESPECIFICACIONES: list = ["exel", "cva", "ct", "syscom"]

CAMPOS_TEXTO: list = [
    "cnombre", "cmarca", "ccategoria",
    "cdescripcion", "cespecificaciones", "cimagen",
]
VALORES_NULOS: set = {"NULL", "null", "None", "nan", "ND", ""}

COLS_TBL_PRODUCTO = [
    "csku", "cnombre", "cmarca", "cdescripcion",
    "cespecificaciones", "cimagen", "bestatus",
    "tcreate_at", "tupdate_at",
]
COLS_TBL_DETALLE = [
    "csku", "nid_proveedor", "ndisponibilidad",
    "cmoneda", "nprecio", "cclave_producto",
]

# ==============================================================================
# UTILIDADES GENERALES
# ==============================================================================
def get_db_engine():
    try:
        missing = [v for v in ["DB_USER", "DB_PASS", "DB_HOST", "DB_PORT", "DB_NAME"]
                   if not os.getenv(v)]
        if missing:
            raise ValueError(f"Variables de entorno faltantes: {missing}")
        db_url = URL.create(
            drivername="postgresql",
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME"),
        )
        return create_engine(db_url, pool_size=10, max_overflow=20)
    except Exception as e:
        logger.error(f"Error conectando a BD: {e}")
        return None


def normalize_text(val: str) -> str:
    if not isinstance(val, str):
        return ""
    val = val.lower()
    val = unicodedata.normalize("NFKD", val).encode("ASCII", "ignore").decode("ASCII")
    val = re.sub(r"(\d+)\s*(ml|cm|mm|kg|g)", r"\1_\2", val)
    val = re.sub(r"[^a-z0-9\s\-_\/]", " ", val)
    return val.strip()


def normalizar_marca(val):
    if pd.isna(val):
        return None
    return re.sub(r"\s+", " ", str(val).strip()).upper()

# ==============================================================================
# PASO 1 - CARGA DE TABLAS TEMPORALES
# ==============================================================================
def cargar_tablas_temporales(engine) -> dict:
    dfs = {}
    for prov, cfg in PROVEEDORES.items():
        tabla = cfg["tabla_temp"]
        try:
            df = pd.read_sql(f"SELECT * FROM {tabla}", engine)
            if df.empty:
                logger.warning(f"[{prov}] {tabla} vacia, se omite.")
            else:
                logger.info(f"[{prov}] {len(df):,} registros cargados de {tabla}.")
                dfs[prov] = df
        except Exception as e:
            logger.error(f"[{prov}] Error leyendo {tabla}: {e}")
    return dfs

# ==============================================================================
# PASO 2 - LIMPIEZA Y NORMALIZACION POR PROVEEDOR
# ==============================================================================
def limpiar_proveedor(df_raw: pd.DataFrame, prov: str) -> pd.DataFrame:
    cfg = PROVEEDORES[prov]
    df = df_raw.copy()

    if "SKU" not in df.columns:
        logger.error(f"[{prov}] Sin columna 'SKU'. Descartando.")
        return pd.DataFrame()

    df["csku"] = df["SKU"].astype(str).str.strip()
    df = df[df["csku"].notna() & ~df["csku"].isin(VALORES_NULOS)]
    df = df.drop_duplicates(subset=["csku"])

    if "ID_PROVEEDOR" not in df.columns:
        logger.error(f"[{prov}] Sin columna 'ID_PROVEEDOR'. Descartando.")
        return pd.DataFrame()

    df["nid_proveedor"] = df["ID_PROVEEDOR"].astype(int)

    rename_map = {}
    rename_map.update(cfg["cols_producto"])
    rename_map.update(cfg["cols_detalle"])
    df = df.rename(columns=rename_map)

    moneda_map = cfg.get("moneda_map", {})
    if moneda_map and "cmoneda" in df.columns:
        df["cmoneda"] = df["cmoneda"].replace(moneda_map)

    cols_texto = df.select_dtypes(include=["object"]).columns
    df[cols_texto] = df[cols_texto].replace(list(VALORES_NULOS), np.nan)

    cols_utiles = (
        ["csku", "nid_proveedor"]
        + [c for c in CAMPOS_TEXTO if c in df.columns]
        + [c for c in ["ndisponibilidad", "nprecio", "cmoneda", "cclave_producto"]
           if c in df.columns]
    )
    return df[cols_utiles].copy()

# ==============================================================================
# PASO 3 - CONSOLIDAR MASTER (tbl_producto)
# ==============================================================================
def consolidar_master(dfs: dict) -> pd.DataFrame:
    provs_disponibles = [p for p in PRIORIDAD if p in dfs]
    if not provs_disponibles:
        logger.error("No hay proveedores para consolidar.")
        return pd.DataFrame()

    partes = []
    for prov in provs_disponibles:
        df = dfs[prov]
        cols_texto_pres = [c for c in CAMPOS_TEXTO if c in df.columns]
        sub = df[["csku"] + cols_texto_pres].set_index("csku")
        sub = sub.add_suffix(f"_{prov}")
        partes.append(sub)

    df_wide = pd.concat(partes, axis=1)

    for campo in CAMPOS_TEXTO:
        prioridad_campo = (
            PRIORIDAD_ESPECIFICACIONES if campo == "cespecificaciones" else PRIORIDAD
        )
        cols = [
            f"{campo}_{p}" for p in prioridad_campo
            if f"{campo}_{p}" in df_wide.columns
        ]
        if not cols:
            continue
        df_wide[campo] = df_wide[cols].bfill(axis=1).iloc[:, 0]
        df_wide.drop(columns=cols, inplace=True)

    df_master = df_wide.reset_index()
    df_master = df_master.dropna(subset=["csku", "cnombre"])

    if "cmarca" in df_master.columns:
        df_master["cmarca"] = df_master["cmarca"].apply(normalizar_marca)

    now = datetime.now()
    df_master["bestatus"] = True
    df_master["tcreate_at"] = now
    df_master["tupdate_at"] = now

    logger.info(f"Master consolidado: {len(df_master):,} SKUs unicos.")
    return df_master

# ==============================================================================
# PASO 4 - ARMAR DETALLE (tbl_detalle_producto)
# ==============================================================================
def armar_detalle(dfs: dict) -> pd.DataFrame:
    bloques = []
    for prov, df in dfs.items():
        cols = [c for c in COLS_TBL_DETALLE if c in df.columns]
        if "csku" not in cols or "nid_proveedor" not in cols:
            logger.warning(f"[{prov}] Sin columnas minimas para detalle, se omite.")
            continue
        bloques.append(df[cols].copy())

    if not bloques:
        logger.error("No hay bloques de detalle para armar.")
        return pd.DataFrame()

    df_det = pd.concat(bloques, ignore_index=True)
    df_det = df_det.dropna(subset=["nprecio"])
    df_det = df_det[~df_det["csku"].isin(VALORES_NULOS)]
    df_det["ndisponibilidad"] = (
        pd.to_numeric(df_det["ndisponibilidad"], errors="coerce").fillna(0).astype(int)
    )
    df_det["nprecio"] = pd.to_numeric(df_det["nprecio"], errors="coerce")
    df_det["nid_proveedor"] = df_det["nid_proveedor"].astype(int)
    df_det = (
        df_det
        .sort_values("ndisponibilidad", ascending=False)
        .drop_duplicates(subset=["csku", "nid_proveedor"], keep="first")
    )

    logger.info(f"Detalle armado: {len(df_det):,} filas (csku x proveedor).")
    return df_det

# ==============================================================================
# PASO 5 - SEPARAR NUEVOS VS EXISTENTES
# ==============================================================================
def separar_nuevos_existentes(df_master, df_det, engine):
    existing_skus = set(
        pd.read_sql("SELECT csku FROM tbl_producto", engine)["csku"]
    )
    mask_new = ~df_master["csku"].isin(existing_skus)
    df_np = df_master[mask_new].copy()
    df_ep = df_master[~mask_new].copy()
    nuevos_skus = set(df_np["csku"])
    df_nd = df_det[df_det["csku"].isin(nuevos_skus)].copy()
    df_ed = df_det[~df_det["csku"].isin(nuevos_skus)].copy()

    logger.info(f"SKUs nuevos: {len(df_np):,}  |  SKUs existentes: {len(df_ep):,}")
    return df_np, df_ep, df_nd, df_ed

# ==============================================================================
# PASO 6 - INSERT NUEVOS PRODUCTOS Y DETALLES
# ==============================================================================
def insertar_nuevos(df_np, df_nd, engine):
    if df_np.empty:
        logger.info("Sin nuevos productos que insertar.")
        return

    cols_prod = [c for c in COLS_TBL_PRODUCTO if c in df_np.columns]
    cols_det  = [c for c in COLS_TBL_DETALLE  if c in df_nd.columns]

    try:
        df_np[cols_prod].to_sql(
            "tbl_producto", engine,
            if_exists="append", index=False, chunksize=2000, method="multi",
        )
        logger.info(f"Insertados {len(df_np):,} nuevos productos.")
    except Exception as e:
        logger.error(f"Error insertando nuevos productos: {e}")

    if not df_nd.empty and cols_det:
        try:
            df_nd[cols_det].to_sql(
                "tbl_detalle_producto", engine,
                if_exists="append", index=False, chunksize=5000, method="multi",
            )
            logger.info(f"Insertados {len(df_nd):,} detalles nuevos.")
        except Exception as e:
            logger.error(f"Error insertando detalles nuevos: {e}")

# ==============================================================================
# PASO 7 + 8 - UPDATE PRODUCTOS EXISTENTES + UPSERT DETALLE
# ==============================================================================
def actualizar_existentes(df_ep, df_ed, engine):
    if not df_ep.empty:
        campos_upd = [
            c for c in ["cnombre", "cmarca", "cdescripcion", "cespecificaciones", "cimagen"]
            if c in df_ep.columns
        ]
        set_sql = ", ".join(f"{c} = %({c})s" for c in campos_upd)
        where_distinct = " OR ".join(
            f"{c} IS DISTINCT FROM %({c})s" for c in campos_upd
        )
        query_prod = f"""
            UPDATE tbl_producto
            SET {set_sql}, tupdate_at = CURRENT_TIMESTAMP
            WHERE csku = %(csku)s
              AND ({where_distinct});
        """
        datos_prod = (
            df_ep[["csku"] + campos_upd]
            .replace({np.nan: None})
            .to_dict("records")
        )
        try:
            with engine.begin() as conn:
                execute_batch(
                    conn.connection.cursor(),
                    query_prod, datos_prod, page_size=1000,
                )
            logger.info(f"Actualizados {len(datos_prod):,} productos existentes.")
        except Exception as e:
            logger.error(f"Error actualizando productos existentes: {e}")
            raise

    if df_ed.empty:
        logger.info("Sin detalles existentes que actualizar.")
        return

    cols_det  = [c for c in COLS_TBL_DETALLE if c in df_ed.columns]
    datos_det = df_ed[cols_det].replace({np.nan: None}).to_dict("records")

    upsert_sql = """
        INSERT INTO tbl_detalle_producto
            (csku, nid_proveedor, ndisponibilidad, cmoneda, nprecio, cclave_producto)
        VALUES
            (%(csku)s, %(nid_proveedor)s, %(ndisponibilidad)s,
             %(cmoneda)s, %(nprecio)s, %(cclave_producto)s)
        ON CONFLICT (csku, nid_proveedor) DO UPDATE SET
            ndisponibilidad = EXCLUDED.ndisponibilidad,
            cmoneda         = EXCLUDED.cmoneda,
            nprecio         = EXCLUDED.nprecio,
            cclave_producto = EXCLUDED.cclave_producto;
    """
    try:
        with engine.begin() as conn:
            execute_batch(
                conn.connection.cursor(),
                upsert_sql, datos_det, page_size=1000,
            )
        logger.info(f"Upsert de {len(datos_det):,} detalles existentes completado.")
    except Exception as e:
        logger.error(f"Error en upsert de detalles: {e}")
        raise

# ==============================================================================
# PASO 9 - NORMALIZAR MARCAS EN tbl_producto
# ==============================================================================
def normalizar_marcas_en_bd(engine):
    """
    Hace UPDATE directo en tbl_producto usando JOIN contra tbl_normalizacion_marcas.
    Relacion uno a uno: cmarca -> cmarca_norm.
    UPPER + TRIM en ambos lados para absorber diferencias de mayusculas y espacios.
    IS DISTINCT FROM evita tocar filas que ya tienen el valor correcto.
    Se ejecuta despues de insertar y actualizar todos los productos.
    """
    sql = """
        UPDATE tbl_producto p
        SET    cmarca     = n.cmarca_norm,
               tupdate_at = CURRENT_TIMESTAMP
        FROM   tbl_normalizacion_marcas n
        WHERE  UPPER(TRIM(p.cmarca)) = UPPER(TRIM(n.cmarca))
          AND  p.cmarca IS DISTINCT FROM n.cmarca_norm;
    """
    try:
        with engine.begin() as conn:
            resultado = conn.execute(text(sql))
        logger.info(
            f"Normalizacion de marcas: {resultado.rowcount} productos actualizados en tbl_producto."
        )
    except Exception as e:
        logger.error(f"Error normalizando marcas en BD: {e}", exc_info=True)

# ==============================================================================
# PASO 10 - TIPO DE CAMBIO USD (Banxico)
# ==============================================================================
def actualizar_tipo_cambio_usd(engine):
    token = os.getenv("BANXICO_TOKEN")
    if not token:
        logger.error("BANXICO_TOKEN no encontrado.")
        return
    url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/SF43718/datos/oportuno"
    try:
        r = requests.get(url, headers={"Bmx-Token": token}, timeout=10)
        r.raise_for_status()
        dato  = float(r.json()["bmx"]["series"][0]["datos"][0]["dato"])
        precio = int(dato) + (1 if dato != int(dato) else 0)
        fecha  = datetime.now()
        with engine.begin() as conn:
            res = conn.execute(
                text("UPDATE tbl_cambio_divisas "
                     "SET precio=:p, fehca_actualizacion=:f WHERE divisa='USD'"),
                {"p": precio, "f": fecha},
            )
            if res.rowcount == 0:
                conn.execute(
                    text("INSERT INTO tbl_cambio_divisas "
                         "(divisa, precio, fehca_actualizacion) VALUES ('USD',:p,:f)"),
                    {"p": precio, "f": fecha},
                )
                logger.info(f"Tipo de cambio USD insertado: ${precio}")
            else:
                logger.info(f"Tipo de cambio USD actualizado: ${precio}")
    except Exception as e:
        logger.error(f"Error Banxico: {e}")

# ==============================================================================
# PASO 11 - PONDERACION DE PRECIO B2B
# ==============================================================================
def ponderacion_de_precio(engine):
    logger.info("Iniciando ponderacion de precio B2B...")
    try:
        tc = dict(
            pd.read_sql("SELECT divisa, precio FROM tbl_cambio_divisas", engine)
            .itertuples(index=False, name=None)
        )
        df = pd.read_sql(
            "SELECT csku, cmoneda, nprecio, ndisponibilidad "
            "FROM tbl_detalle_producto "
            "WHERE nprecio > 0 AND ndisponibilidad > 0",
            engine,
        )
        if df.empty:
            logger.warning("Sin precios para ponderar.")
            return

        df["precio_mxn"] = df["cmoneda"].map(tc).fillna(1) * df["nprecio"]

        resultados = []
        for sku, g in df.groupby("csku"):
            precios = g["precio_mxn"].values
            disp    = g["ndisponibilidad"].values
            mu      = precios.mean()
            sigma   = precios.std() or mu * 0.05
            pesos   = np.exp(-((precios - mu) ** 2) / (2 * sigma ** 2)) * disp
            suma    = pesos.sum()
            costo   = np.dot(precios, pesos) / suma if suma > 0 else mu
            costo_b2b = costo / 0.92 * 1.16
            costo_b2b = int(costo_b2b) + (1 if costo_b2b != int(costo_b2b) else 0)
            resultados.append((costo_b2b, int(disp.sum()), sku))

        with engine.begin() as conn:
            execute_batch(
                conn.connection.cursor(),
                "UPDATE tbl_producto "
                "SET nprecio_b2b = %s, ndisponibilidad_total = %s, tupdate_at = CURRENT_TIMESTAMP "
                "WHERE csku = %s",
                resultados, page_size=1000,
            )
        logger.info(f"Ponderacion aplicada a {len(resultados):,} SKUs.")
    except Exception as e:
        logger.error(f"Error en ponderacion: {e}")

# ==============================================================================
# PASO 12 - ESTATUS ACTIVO / INACTIVO
# ==============================================================================
def actualizar_estatus_productos(engine):
    try:
        with engine.begin() as conn:
            r_off = conn.execute(text("""
                UPDATE tbl_producto
                SET bestatus = true
                WHERE ndisponibilidad_total IS NULL
                   OR ndisponibilidad_total = 0
                   OR csku NOT IN (SELECT DISTINCT csku FROM tbl_detalle_producto)
            """))
            r_on = conn.execute(text("""
                UPDATE tbl_producto
                SET bestatus = true
                WHERE ndisponibilidad_total > 0
            """))
        logger.info(
            f"Estatus: {r_off.rowcount} desactivados | {r_on.rowcount} activados."
        )
    except Exception as e:
        logger.error(f"Error actualizando estatus: {e}")

# ==============================================================================
# PASO 13 - CATEGORIZADOR DEEP LEARNING
# ==============================================================================
def categorizador_deep_learning(engine):
    logger.info("Iniciando categorizador Deep Learning...")
    model = tokenizer = le = df = X = None
    try:
        import tensorflow as tf
        from tensorflow.keras.utils import pad_sequences

        df = pd.read_sql(
            "SELECT csku, cnombre, cdescripcion, cmarca "
            "FROM tbl_producto WHERE nid_subcategoria IS NULL",
            engine,
        )
        if df.empty:
            logger.info("Todos los productos ya tienen subcategoria.")
            return

        base  = os.path.abspath(os.getcwd())
        model = tf.keras.models.load_model(
            os.path.join(base, "Red_neuronal", "modelo_categorias.keras")
        )
        with open(os.path.join(base, "Red_neuronal", "tokenizer.pkl"), "rb") as f:
            tokenizer = pickle.load(f)
        with open(os.path.join(base, "Red_neuronal", "labelencoder.pkl"), "rb") as f:
            le = pickle.load(f)

        df["texto_norm"] = (
            df["cnombre"].fillna("") + " " +
            df["cdescripcion"].fillna("") + " " +
            df["cmarca"].fillna("")
        ).apply(normalize_text)

        X = pad_sequences(
            tokenizer.texts_to_sequences(df["texto_norm"]), maxlen=300
        )
        df["cat_pred"] = le.inverse_transform(
            np.argmax(model.predict(X, batch_size=32), axis=1)
        )

        df_sub = pd.read_sql(
            "SELECT nid AS id_sub, cnombre_subcategoria FROM tbl_subcategoria", engine
        )
        df = df.merge(
            df_sub, left_on="cat_pred", right_on="cnombre_subcategoria", how="left"
        )

        updates = [
            (row.id_sub, row.csku)
            for row in df.itertuples()
            if pd.notna(row.id_sub)
        ]
        if updates:
            with engine.begin() as conn:
                execute_batch(
                    conn.connection.cursor(),
                    "UPDATE tbl_producto SET nid_subcategoria = %s WHERE csku = %s",
                    updates, page_size=1000,
                )
            logger.info(f"{len(updates):,} productos categorizados.")
    except Exception as e:
        logger.error(f"Error en categorizador NLP: {e}")
    finally:
        del model, tokenizer, le, df, X
        try:
            import tensorflow as tf
            tf.keras.backend.clear_session()
        except Exception:
            pass
        gc.collect()

# ==============================================================================
# MAIN
# ==============================================================================
def main():
    t0 = time.time()
    logger.info("INICIANDO ETL UNIFICADO - TODOS LOS MAYORISTAS")

    engine = get_db_engine()
    if engine is None:
        logger.critical("Sin conexion a BD. Abortando.")
        sys.exit(1)

    # 1. Cargar
    dfs_raw = cargar_tablas_temporales(engine)
    if not dfs_raw:
        logger.warning("Ninguna tabla temporal tiene datos. Fin.")
        engine.dispose()
        return

    # 2. Limpiar
    dfs_limpios = {}
    for prov, df in dfs_raw.items():
        limpio = limpiar_proveedor(df, prov)
        if not limpio.empty:
            dfs_limpios[prov] = limpio
    del dfs_raw
    gc.collect()

    if not dfs_limpios:
        logger.error("Todos los DataFrames vacios tras limpieza. Abortando.")
        engine.dispose()
        return

    # 3. Consolidar master
    df_master = consolidar_master(dfs_limpios)
    if df_master.empty:
        logger.error("Master vacio. Abortando.")
        engine.dispose()
        return

    # 4. Armar detalle
    df_det = armar_detalle(dfs_limpios)
    del dfs_limpios
    gc.collect()

    # 5. Separar nuevos / existentes
    df_np, df_ep, df_nd, df_ed = separar_nuevos_existentes(df_master, df_det, engine)
    del df_master, df_det
    gc.collect()

    # 6. Insertar nuevos
    insertar_nuevos(df_np, df_nd, engine)
    del df_np, df_nd
    gc.collect()

    # 7 + 8. Actualizar existentes
    actualizar_existentes(df_ep, df_ed, engine)
    del df_ep, df_ed
    gc.collect()

    # 9. Normalizar marcas en BD
    logger.info("Normalizando marcas en tbl_producto...")
    normalizar_marcas_en_bd(engine)

    # 10. Tipo de cambio
    logger.info("Actualizando tipo de cambio USD...")
    actualizar_tipo_cambio_usd(engine)

    # 11. Ponderacion B2B
    logger.info("Ponderando precio B2B...")
    ponderacion_de_precio(engine)

    # 12. Estatus
    logger.info("Sincronizando bestatus...")
    actualizar_estatus_productos(engine)

    # 13. Categorizador NLP
    logger.info("Categorizando con Deep Learning...")
    categorizador_deep_learning(engine)

    engine.dispose()
    logger.info(f"ETL FINALIZADO en {time.time() - t0:.2f}s")


if __name__ == "__main__":
    main()