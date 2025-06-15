import pandas as pd
import gspread
import yaml
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import re
import unicodedata
import uuid
from datetime import datetime, timezone, timedelta
import pytz


def cargar_configuracion(ruta):
    with open(ruta) as f:
        return yaml.safe_load(f)

def leer_google_sheet(config: dict) -> pd.DataFrame:
    creds = Credentials.from_service_account_file(
        config["credenciales"],
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
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
    nombre = re.sub(r"[^\w]", "_", nombre)
    nombre = re.sub(r"_+", "_", nombre)
    return nombre.strip("_")[:300]

def one_hot_listas(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    for col in columnas:
        if col not in df.columns:
            print(f"⚠️  La columna '{col}' no existe en el DataFrame.")
            continue

        df[col] = df[col].fillna("").astype(str)
        df[col] = df[col].apply(lambda x: [item.strip() for item in x.split(",") if item.strip()])

        exploded = df[[col]].explode(col)
        dummies = pd.get_dummies(exploded[col])
        dummies.columns = [normalizar_nombre(f"{col}_{v}") for v in dummies.columns]
        dummies = dummies.groupby(exploded.index).sum()
        df = pd.concat([df.drop(columns=[col]), dummies], axis=1)

    return df

def agregar_uuid(df: pd.DataFrame, nombre_columna: str = "uuid") -> pd.DataFrame:
    df[nombre_columna] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def agregar_timestamp(df: pd.DataFrame, nombre_columna: str = "cargado_en") -> pd.DataFrame:
    zona_colombia = pytz.timezone("America/Bogota")
    df[nombre_columna] = pd.Timestamp(datetime.now(zona_colombia))
    return df

def ajustar_tipos(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    formato_fecha = "%d/%m/%Y"
    columnas_fecha = config.get("columnas_fecha", [])
    columnas_float = config.get("columnas_float", [])
    columnas_listas = config.get("columnas_listas", [])  # prefijos de columnas one-hot

    for col in df.columns:
        if col in columnas_fecha:
            try:
                df[col] = pd.to_datetime(df[col], format=formato_fecha, errors="coerce").dt.round("us")
                print(f"✅ '{col}' convertida a datetime")
            except Exception as e:
                print(f"❌ Error al convertir '{col}' a datetime: {e}")
        elif col in columnas_float:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                print(f"✅ '{col}' convertida a float")
            except Exception as e:
                print(f"❌ Error al convertir '{col}' a float: {e}")
        elif not any(col.startswith(p + "_") for p in columnas_listas):
            df[col] = df[col].astype(str)

    return df


def expandir_listas_onehot(df: pd.DataFrame, columnas_id: list, prefijos: list) -> pd.DataFrame:
    registros = []

    # Forzar columnas one-hot a tipo int64 (0 o 1)
    for col in df.columns:
        if any(col.startswith(f"{prefijo}_") for prefijo in prefijos):
            df[col] = df[col].fillna(0).astype("int64")

    for _, row in df.iterrows():
        for prefijo in prefijos:
            columnas = [col for col in df.columns if col.startswith(f"{prefijo}_")]
            for col in columnas:
                if row.get(col) == 1:
                    valor = col.replace(f"{prefijo}_", "")
                    registro = row[columnas_id].to_dict()
                    contexto_extra = row.drop(labels=[c for c in df.columns if c.startswith(tuple(prefijos))], errors='ignore')
                    registro.update(contexto_extra.to_dict())
                    registro["variables_onehot"] = prefijo
                    registro["valor_onehot"] = valor
                    registros.append(registro)

    return pd.DataFrame(registros)


def cargar_a_bigquery(df, config):
    proyecto = config["bigquery"]["proyecto"]
    dataset = config["bigquery"]["dataset"]
    tabla = config["bigquery"]["tabla_destino"]
    tabla_destino = f"{dataset}.{tabla}"
    ruta_credenciales = config["credenciales"]

    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.round("us")

    credentials = service_account.Credentials.from_service_account_file(ruta_credenciales)

    print(f"📤 Subiendo a BigQuery tabla: {tabla_destino} (proyecto: {proyecto})")

    to_gbq(
        df,
        destination_table=tabla_destino,
        project_id=proyecto,
        if_exists="replace",
        credentials=credentials
    )

    print(f"✅ Tabla {tabla_destino} subida correctamente con {len(df)} registros.")
