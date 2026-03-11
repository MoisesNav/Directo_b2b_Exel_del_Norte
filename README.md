# Procesamiento de Catálogo de Productos - Exel del Norte

## Descripción

Este repositorio contiene un conjunto de rutinas desarrolladas en **Python** para el procesamiento automatizado del catálogo de productos del proveedor **Exel del Norte**.

El sistema implementa un pipeline de **ETL (Extract, Transform, Load)** permitiendo integrar, procesar y almacenar la información de productos en una base de datos estructurada para su posterior uso en plataformas de eCommerce, análisis de datos y sistemas de inventario.

El proceso incluye extracción de datos, limpieza y normalización, categorización automática mediante un modelo de **red neuronal**, obtención de fichas técnicas de los producto dada la conexcion con **Icecat** y carga optimizada en la base de datos.

---

## Funcionalidades

- **Extracción de datos**
  - Consumo del catálogo de productos del proveedor.
  - Procesamiento de información proveniente de archivos o API.

- **Transformación de datos**
  - Limpieza y normalización de nombres y descripciones.
  - Estandarización de marcas y atributos.
  - Preparación de datos para modelos de clasificación.

- **Categorización automática**
  - Clasificación de productos utilizando un modelo de **red neuronal entrenado previamente**.
  - Uso de **tokenizer y label encoder** para el procesamiento del texto.

- **Obtención de fichas técnicas**
  - Obtencion de los json descriptivos y de fichas tecnicas del proveedor **Icecat** y asociación de especificaciones técnicas de los productos.

- **Carga en base de datos**
  - Inserción y actualización de productos en **PostgreSQL**.
  - Optimización del rendimiento mediante operaciones batch.

---

## Arquitectura del proceso
- Extracción de catálogo
    ↓
- Limpieza y normalización
    ↓
- Categorización con Red Neuronal
    ↓
- Obtención de fichas técnicas
    ↓
- Carga en Base de Datos


---

## Tecnologías utilizadas

- **Python**
- **Pandas**
- **PostgreSQL**
- **TensorFlow / Keras**
- **APIs REST**
- **Procesamiento concurrente**

