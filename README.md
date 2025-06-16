
# ETL del Observatorio INO

Este sistema ETL extrae, transforma y carga (ETL) datos desde hojas de Google Sheets a BigQuery, aplicando renombramiento forzado, limpieza de datos, tipificación explícita y transformación a formato largo cuando se requiere.

## 🔧 Características principales

- 🧾 **Renombramiento forzado** de columnas según el orden definido en la configuración.
- 🧮 **Tipificación controlada**:
  - `columnas_fecha`: convertidas a `datetime` (formato `%d/%m/%Y`)
  - `columnas_float`: convertidas a `float`
  - Todas las demás columnas se convierten explícitamente a `string`
  - Columnas de codificación one-hot (0/1) se fuerzan a `int64`
- 🧠 **Codificación one-hot** de columnas con listas separadas por comas (definidas en `columnas_listas`)
- 🔁 **Expansión opcional a formato largo** con los campos `tipo` y `valor` (solo en el proceso de productos)
- 🪪 **Asignación de UUID único** a cada fila original
- 🕒 **Agregado de timestamp de carga** con hora local de Colombia (`America/Bogota`)
- 🗃️ **Carga segura en BigQuery**: reemplaza la tabla por completo en cada ejecución (`if_exists="replace"`)
- 🔐 **Autenticación mediante credenciales de servicio** (archivo `.json`)

## 🧱 Estructura del repositorio

- `etl_productos.py`: procesa la hoja `Producción`, incluyendo codificación y expansión a formato largo.
- `etl_proyectos.py`: procesa la hoja `Proyectos`, sin transformación a formato largo.
- `common.py`: funciones reutilizables (lectura, transformación, carga, utilidades).
- `main.py`: ejecuta ambos procesos en orden.
- `config_productos.yaml` y `config_proyectos.yaml`: archivos de configuración separados para cada hoja.

## 📦 Configuración YAML esperada

```yaml
google_sheet:
  spreadsheet_id: "<ID del documento>"
  worksheet_name: "<Nombre de la hoja>"

credenciales: "/ruta/a/credenciales.json"

columnas_productos:  # o columnas_proyectos
  - col1
  - col2
  # ...

columnas_listas:  # Solo si se aplica one-hot + long
  - colaboraciones
  - indexacion
  - staff_ino

columnas_fecha:
  - fecha_cei
  - fecha_ctc

columnas_float:
  - muestra_tamanio
  - muestra_inicial

bigquery:
  proyecto: "mi-proyecto"
  dataset: "mi_dataset"
  tabla_destino: "mi_tabla"
```

## 🧰 Requisitos

- Python 3.10+
- Instala dependencias con:
```bash
pip install -r requirements.txt
```

## ▶️ Uso

```bash
python main.py
```

Esto ejecutará los procesos ETL para `Producción` y `Proyectos`.

## 📝 Salida esperada en BigQuery

**Para productos**: tabla en formato largo con `uuid`, campos contextuales, `tipo`, `valor`, `cargado_en`.

**Para proyectos**: tabla directa con filas limpias, tipificadas y sin expansión.

## 📬 Contacto

Para soporte, contactar al equipo de datos del Observatorio INO.
