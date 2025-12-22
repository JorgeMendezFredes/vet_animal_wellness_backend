# Vet Animal Wellness Backend API

Este proyecto es el backend API para `vet_animal_wellness`.
Está construido con FastAPI y Python.
El objetivo es proveer datos para el dashboard del frontend, utilizando datos procesados de `vetpraxis-bi`.

## Instalación

1.  Crear un entorno virtual:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  Instalar dependencias:
    ```bash
    pip install -r requirements.txt
    ```

3.  Ejecutar el servidor:
    ```bash
    uvicorn main:app --reload
    ```

## Despliegue en Render

Este proyecto está configurado para desplegarse en Render.
