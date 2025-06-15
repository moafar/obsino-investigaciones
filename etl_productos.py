from common import *

def ejecutar_etl_productos():
    
    config = cargar_configuracion("config_productos.yaml")# Cargar configuración desde el YAML
    df = leer_google_sheet(config)# Leer datos desde Google Sheets   
    df = renombrar_columnas(df, config.get("columnas_productos", []))# Forzar renombramiento de columnas por posición
    df = one_hot_listas(df, config.get("columnas_listas", []))# One-hot encoding a columnas tipo lista (ya con nombres renombrados)
    df = ajustar_tipos(df, config) # Ajuste de tipos
    df = agregar_uuid(df, nombre_columna="uuid")# Agregar UUID único por fila original (por proyecto)

    # Expandir listas a formato largo
    df_largo = expandir_listas_onehot(
        df,
        columnas_id=["estudio_cod", "uuid"],
        prefijos=["colaboraciones", "indexacion", "staff_ino"]
    )

   
    df_largo = agregar_timestamp(df_largo) # Agregar timestamp de carga
    cargar_a_bigquery(df_largo, config) # Cargar a BigQuery (sobrescribe tabla completa o la crea si no existe)
    print(f"Carga de PRODUCTOS finalizada.  DTYPES: {df_largo.dtypes}")

def main():
    ejecutar_etl_productos()

if __name__ == "__main__":
    main()
