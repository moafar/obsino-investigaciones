from common import *

def ejecutar_etl_proyectos():
    config = cargar_configuracion("config_proyectos.yaml")
    df = leer_google_sheet(config)
    df = renombrar_columnas(df, config.get("columnas_proyectos", []))
    df = ajustar_tipos(df, config)
    df = agregar_uuid(df)
    df = agregar_timestamp(df)
    cargar_a_bigquery(df, config)
    print(f"Carga de PROYECTOS finalizada.  DTYPES: {df.dtypes}")

def main():
    ejecutar_etl_proyectos()

if __name__ == "__main__":
    main()
