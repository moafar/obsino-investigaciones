import pandas as pd
import gspread
import yaml
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from pandas_gbq import to_gbq
from google.oauth2 import service_account
from pandas_gbq import to_gbq
import re
import unicodedata
import uuid
from datetime import datetime, timezone
import pytz


def cargar_configuracion(ruta="config_productos.yaml"):
    with open(ruta) as f:
        return yaml.safe_load(f)

def leer_google_sheet(config):
    creds = Credentials.from_service_account_file(config["credenciales"], scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(config["google_sheet"]["spreadsheet_id"])
    data = sheet.worksheet(config["google_sheet"]["worksheet_name"]).get_all_records()
    return pd.DataFrame(data)

def renombrar_columnas(df: pd.DataFrame, nuevos_nombres: list) -> pd.DataFrame:
    if len(df.columns) != len(nuevos_nombres):
        print("⚠️ No se renombraron las columnas: número de columnas no coincide.")
        print(f"→ Columnas actuales: {len(df.columns)}, nombres esperados: {len(nuevos_nombres)}")
        return df
    else:
        df.columns = nuevos_nombres
        print("✅ Columnas renombradas forzadamente por orden.")
        return df

def normalizar_nombre(nombre: str) -> str:
    nombre = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    nombre = re.sub(r"[^\w]", "_", nombre)         # Reemplaza todo lo que no sea A-Za-z0-9_
    nombre = re.sub(r"_+", "_", nombre)            # Reemplaza múltiples _ por uno solo
    return nombre.strip("_")[:300]                 # Asegura que no empiece/termine en _ y limite 300

def one_hot_listas(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    for col in columnas:
        if col not in df.columns:
            print(f"⚠️  La columna '{col}' no existe en el DataFrame.")
            continue

        # Asegura tipo string y separa en listas
        df[col] = df[col].fillna("").astype(str)
        df[col] = df[col].apply(lambda x: [item.strip() for item in x.split(",") if item.strip()])

        # Explota la lista en filas individuales
        exploded = df[[col]].explode(col)

        # Dummies con prefijo, aún sin sanitizar
        dummies = pd.get_dummies(exploded[col])

        # Renombrar columnas con prefijo y limpieza
        dummies.columns = [normalizar_nombre(f"{col}_{v}") for v in dummies.columns]

        # Volver a agrupar al índice original
        dummies = dummies.groupby(exploded.index).sum()

        # Añadir al DataFrame original
        df = pd.concat([df.drop(columns=[col]), dummies], axis=1)

    return df

def agregar_uuid(df: pd.DataFrame, nombre_columna: str = "uuid") -> pd.DataFrame:
    df[nombre_columna] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def agregar_timestamp(df: pd.DataFrame, nombre_columna: str = "cargado_en") -> pd.DataFrame:
    zona_colombia = pytz.timezone("America/Bogota")
    df[nombre_columna] = pd.Timestamp(datetime.now(zona_colombia))
    return df

def expandir_listas_onehot(df: pd.DataFrame, columnas_id: list, prefijos: list) -> pd.DataFrame:
    registros = []

    for _, row in df.iterrows():
        for prefijo in prefijos:
            columnas = [col for col in df.columns if col.startswith(f"{prefijo}_")]
            for col in columnas:
                if row.get(col) == 1:
                    valor = col.replace(f"{prefijo}_", "")
                    registro = row[columnas_id].to_dict()
                    # Añadir también todos los campos contextuales (excepto los one-hot y columnas de listas)
                    contexto_extra = row.drop(labels=[c for c in df.columns if c.startswith(tuple(prefijos))], errors='ignore')
                    registro.update(contexto_extra.to_dict())
                    registro["tipo"] = prefijo
                    registro["valor"] = valor
                    registros.append(registro)

    return pd.DataFrame(registros)

def ajustar_tipos(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    formato_fecha = "%d/%m/%Y"
    columnas_fecha = config.get("columnas_fecha", [])

    for col in df.columns:
        if col in columnas_fecha:
            try:
                df[col] = pd.to_datetime(df[col], format=formato_fecha, errors="coerce")
                df[col] = df[col].dt.round("us")
                print(f"✅ '{col}' convertida a datetime")
            except Exception as e:
                print(f"❌ Error al convertir '{col}': {e}")
        elif pd.api.types.is_datetime64_ns_dtype(df[col]):
            df[col] = df[col].dt.round("us")
    return df


def cargar_a_bigquery(df, config):
    proyecto = config["bigquery"]["proyecto"]
    dataset = config["bigquery"]["dataset"]
    tabla = config["bigquery"]["tabla_destino"]
    tabla_destino = f"{dataset}.{tabla}"
    ruta_credenciales = config["credenciales"]

    # Convertir datetime64[ns] → datetime64[us]
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.round("us")

    # Crear objeto de credenciales
    credentials = service_account.Credentials.from_service_account_file(ruta_credenciales)

    print(f"📤 Subiendo a BigQuery tabla: {tabla_destino} (proyecto: {proyecto})")

    to_gbq(
        df,
        destination_table=tabla_destino,
        project_id=proyecto,
        if_exists="replace",  # sobrescribe la tabla
        credentials=credentials
    )

    print(f"✅ Tabla {tabla_destino} subida correctamente con {len(df)} registros.")

def main():
    # Cargar configuración desde el YAML
    config = cargar_configuracion()

    # Leer datos desde Google Sheets
    df = leer_google_sheet(config)

    # Forzar renombramiento de columnas por posición
    df = renombrar_columnas(df, config.get("columnas_productos", []))

    # One-hot encoding a columnas tipo lista (ya con nombres renombrados)
    df = one_hot_listas(df, config.get("columnas_listas", []))

    # Ajuste de tipos (opcional: puedes mejorarlo con reglas más específicas si hace falta)
    df = ajustar_tipos(df, config)

    # Agregar UUID único por fila original (por proyecto)
    df = agregar_uuid(df, nombre_columna="uuid")

    # Suponiendo que las columnas identificadoras sean "estudio_cod"
    df_largo = expandir_listas_onehot(
        df,
        columnas_id=["estudio_cod", "uuid"], 
        prefijos=["colaboraciones", "indexacion", "staff_ino"]
    )

    # Agregar timestamp de carga
    df_largo = agregar_timestamp(df_largo)

    # Cargar a BigQuery (sobrescribe tabla completa o la crea si no existe)
    cargar_a_bigquery(df_largo, config)


if __name__ == "__main__":
    main()
