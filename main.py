from etl_productos import ejecutar_etl_productos
from etl_proyectos import ejecutar_etl_proyectos


def main():
    print("\n🔄 Ejecutando ETL para la hoja 'Producción'")
    ejecutar_etl_productos()

    print("\nEjecutando ETL para la hoja 'Proyectos'...")
    ejecutar_etl_proyectos()


if __name__ == "__main__":
    main()
