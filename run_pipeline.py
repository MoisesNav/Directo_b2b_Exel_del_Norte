# ==========================================================
# PROCESO: ORQUESTACIÓN PIPELINE B2B
#   - Coordinar ejecución secuencial de procesos ETL
#   - Controlar flujo de ejecución y dependencias
#   - Registrar eventos y tiempos de ejecución
#   - Detener pipeline ante fallos críticos
#
# FLUJO:
#   1. Extracción proveedor (Exel  tabla temporal)
#   2. Proceso ETL (merge, precios, ML, estatus)
#   3. Enriquecimiento Icecat
#
# SALIDAS:
#   - Logs de ejecución
#   - Estado de ejecución del pipeline
#
# PROCESOS CLAVE:
#   - Ejecución de scripts como subprocesos
#   - Manejo de errores críticos
#   - Medición de tiempos
# ==========================================================

import subprocess
import sys
import time
import logging

# Configurar logging global del pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s -  [PIPELINE] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_execution.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# EJECUCIÓN DE SCRIPTS
def run_script(script_name):
    """Ejecutar script Python como subproceso y controlar errores."""
    
    logger.info(f"Iniciando: {script_name}...")
    start_time = time.time()
    
    try:
        # Ejecutar script externo
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,
            text=True,
            capture_output=False
        )

        elapsed = time.time() - start_time
        logger.info(f"Finalizado con éxito: {script_name} ({elapsed:.2f}s)")
        return True

    except subprocess.CalledProcessError:
        logger.error(f"Error crítico en {script_name}. Detener pipeline.")
        return False

# FUNCIÓN PRINCIPAL
def main():
    pipeline_start = time.time()

    logger.info("=== INICIANDO RUTINA DIARIA DE ACTUALIZACIÓN DIRECTOB2B ===")

    # Extracción y carga temporal
    if not run_script("ob_cat_exel.py"):
        sys.exit(1)

    # Proceso ETL principal
    if not run_script("etl.py"):
        sys.exit(1)

    # Enriquecimiento Icecat (no crítico)
    if not run_script("fichas_masivas_icecta_seg.py"):
        logger.warning("Icecat no completado, pipeline continúa.")

    total_time = time.time() - pipeline_start

    logger.info(f"=== PIPELINE FINALIZADO EXITOSAMENTE en {total_time/60:.2f} minutos ===")

# Ejecución
if __name__ == "__main__":
    main()