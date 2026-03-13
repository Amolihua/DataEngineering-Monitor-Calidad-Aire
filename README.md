# 🌍 Data Engineering con OpenAQ

Este proyecto implementa una arquitectura de datos ) para el monitoreo de la calidad del aire en tiempo real, utilizando la API de OpenAQ. El sistema está diseñado para ser escalable, incremental y automatizado mediante una arquitectura en Databricks.

## Descripción del Proyecto
El objetivo es ingerir, transformar y analizar datos de contaminantes atmosféricos (PM2.5, CO, NO2, etc.) de múltiples ciudades, permitiendo la detección de picos anómalos y la generación de métricas para análisis de datos y Machine Learning.

## Arquitectura de Datos Medallion 
El pipeline utiliza Delta Live Tables (DLT) para gestionar el flujo de datos:

Bronze Layer: Ingesta incremental de JSONs crudos desde la API de OpenAQ utilizando Auto Loader (cloudFiles). Incluye metadatos de linaje de archivos mediante _metadata.file_path.

Silver Layer: Limpieza de tipos de datos (cast de Timestamp), filtrado de valores nulos o negativos, y unificación de múltiples fuentes en una tabla maestra global.

Gold Layer: Generación de tablas de negocio listas para consumo:

GOLD_DAILY_SUMMARY: Agregaciones de promedios, máximos y mínimos diarios.

GOLD_GAS_ANOMALIES: Identificación de picos críticos por ciudad, basado en desviaciones del promedio horario.

GOLD_ML_FEATURES: Tabla pivotada optimizada para entrenamiento de modelos predictivos.

## Stack Tecnológico
Lenguajes: Python (PySpark), SQL.

Plataforma de Datos: Databricks (Serverless Compute).

Motor de Almacenamiento: Delta Lake & Unity Catalog.

Orquestación: Databricks Workflows (Jobs) programados.

Control de Versiones: Git con integración directa a Databricks.

## Automatización y CI/CD
El proyecto está configurado para ejecutarse de forma autónoma:

Job de Ingesta: Un notebook de Python descarga los datos diarios de la API y los deposita en un Volume de Unity Catalog.

Trigger de Pipeline: Una vez finalizada la ingesta, se dispara el pipeline de Delta Live Tables para procesar los nuevos registros de forma incremental.

Integración con GitHub: Los Workflows y Pipelines leen directamente desde este repositorio, garantizando que el código en producción sea siempre la versión aprobada en main.
