import sys
import pandas as pd


def main(input_file: str) -> None:

    df = pd.read_parquet(input_file)

    required = {"tpep_pickup_datetime", "PULocationID"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {sorted(missing)}")


    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])

    if "pickup_latitude" not in df.columns:
        df["pickup_latitude"] = pd.NA
    if "pickup_longitude" not in df.columns:
        df["pickup_longitude"] = pd.NA


    base = df[["tpep_pickup_datetime", "PULocationID", "pickup_latitude", "pickup_longitude"]].copy()

    ultima_ubicacion = (
        base.sort_values("tpep_pickup_datetime")
            .groupby("PULocationID", as_index=False)
            .last()
            .rename(columns={
                "PULocationID": "vehicle_id",
                "tpep_pickup_datetime": "ultimo_timestamp",
                "pickup_latitude": "ultima_latitud",
                "pickup_longitude": "ultima_longitud",
            })
    )

    ultima_ubicacion = ultima_ubicacion[["vehicle_id", "ultimo_timestamp", "ultima_latitud", "ultima_longitud"]]
    ultima_ubicacion.to_csv("ultima_ubicacion.csv", index=False)

    df["hora_del_dia"] = df["tpep_pickup_datetime"].dt.hour

    viajes_por_hora = (
        df.groupby("hora_del_dia")
          .size()
          .reset_index(name="total_viajes")
          .sort_values("hora_del_dia")
    )

    viajes_por_hora.to_csv("viajes_por_hora.csv", index=False)

    print("✅ Proceso finalizado correctamente")
    print("Archivos generados:")
    print("- ultima_ubicacion.csv")
    print("- viajes_por_hora.csv")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python process_data.py <archivo.parquet>")
        raise SystemExit(1)

    main(sys.argv[1])
