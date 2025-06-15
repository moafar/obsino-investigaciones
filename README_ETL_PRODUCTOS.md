# ETL de Productos de Investigación

Este script extrae, transforma y carga (ETL) los datos de productos de investigación desde una hoja de cálculo de Google Sheets a una tabla en BigQuery, aplicando limpieza, codificación y normalización.

## Características principales

- 🔄 **Renombramiento forzado** de columnas según orden esperado.
- 🔍 **Limpieza y codificación one-hot** de columnas tipo lista (`colaboraciones`, `indexacion`, `staff_ino`).
- 🔁 **Expansión a formato largo**, manteniendo todos los campos contextuales (como `titulo`, `categoria`, `fecha_publicacion`, etc.) en cada fila expandida.
- 🧾 **Conversión controlada de fechas**, solo para las columnas especificadas en `columnas_fecha` del YAML.
- 🪪 **Agregado de UUID único** por fila original.
- 🕒 **Inclusión de timestamp de carga** (`cargado_en`) con hora local de Colombia.
- 🗃️ **Carga segura y completa en BigQuery**, usando el modo `replace`.
- 🔐 Uso de credenciales de servicio de Google Cloud.
- 👀 Validación interactiva con resumen por tipo y valor antes de la carga.

## Detalles sobre la carga (`replace`)

El script utiliza:

```python
to_gbq(..., if_exists="replace", ...)
```

Esto **no ejecuta un TRUNCATE**, sino que:
- Elimina completamente la tabla existente en BigQuery.
- Crea una nueva tabla con el esquema **inferido desde el DataFrame**.
- Esto asegura que errores de tipo anteriores no persistan en futuras cargas.

## Archivos esperados

- `config_productos.yaml`: archivo YAML con configuración del proyecto.
- Script principal: `etl_productos.py`.

## Requisitos

- Python 3.8+
- Paquetes: `pandas`, `gspread`, `google-auth`, `pandas_gbq`, `pyyaml`, `google-cloud-bigquery`, `pytz`

Instalación:
```bash
pip install -r requirements.txt
```

## Uso

```bash
python etl_productos.py
```

El script solicitará confirmación antes de cargar los datos a BigQuery.

## Salida esperada en BigQuery

Una tabla en formato largo con los siguientes campos:

- `uuid`: identificador único por proyecto
- `estudio_cod`: código del estudio
- `titulo`, `categoria`, `fecha_publicacion`, etc.: todos los campos contextuales del proyecto original
- `tipo`: nombre del atributo expandido (`colaboraciones`, `indexacion`, etc.)
- `valor`: valor individual asociado
- `cargado_en`: timestamp en hora local de Colombia (`America/Bogota`)

## Contacto

Para soporte, contactar al autor del script o equipo de datos del Observatorio INO.