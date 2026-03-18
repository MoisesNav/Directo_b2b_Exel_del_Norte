# DIRECTOB2B/run_pipeline.py
import subprocess
import sys
import time
import logging

# Configuración de logs para el pipeline global
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s -  [PIPELINE] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_execution.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_name):
    """Ejecuta un script de Python y captura su salida."""
    logger.info(f"Iniciando: {script_name}...")
    start_time = time.time()
    
    try:
        # Ejecutamos el script como un subproceso
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            text=True,
            capture_output=False # Cambiar a True si prefieres no ver el log en tiempo real
        )
        elapsed = time.time() - start_time
        logger.info(f" Finalizado con éxito: {script_name} ({elapsed:.2f}s)")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f" Error crítico en {script_name}. El proceso se detendrá.")
        return False

def main():
    pipeline_start = time.time()
    logger.info("=== INICIANDO RUTINA DIARIA DE ACTUALIZACIÓN DIRECTOB2B ===")

    # PASO 1: Extracción y Carga a Temporal (Exel)
    # Es la fuente de verdad. Si falla, no hay nada nuevo que procesar.
    if not run_script("ob_cat_exel.py"):
        sys.exit(1)

    # PASO 2: Merge ETL (Lógica B2B, Precios, Divisas y ML)
    # Este script procesa la tabla temporal y la lleva a la producción.
    if not run_script("etl.py"):
        sys.exit(1)

    # PASO 3: Enriquecimiento de Fichas (Icecat)
    # Es un proceso de mejora. Si falla, el catálogo ya está actualizado en precios,
    # por lo que no detenemos el sistema, solo lo reportamos.
    if not run_script("fichas_masivas_icecta_seg.py"):
        logger.warning("El enriquecimiento de Icecat no se completó, pero los precios están al día.")

    total_time = time.time() - pipeline_start
    logger.info(f"=== PIPELINE FINALIZADO EXITOSAMENTE en {total_time/60:.2f} minutos ===")

if __name__ == "__main__":
    main()