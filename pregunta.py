"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    df= pd.read_fwf(
        "clusters_report.txt",
        widths=[9, 16, 16, 80],
        header=None
    )

    list_col = df[:2].fillna('').apply(lambda x: ' ' + x).sum().tolist()
    list_col = [col.strip().lower().replace(' ', '_') for col in list_col]

    df = df[3:]
    df.columns = list_col

    df = df.fillna(method='ffill')

    df = df.groupby([
        'cluster',
        'cantidad_depalabras_clave',
        'porcentaje_depalabras_clave'
    ],
        as_index=False
    )[[
        'principales_palabras_clave'
    ]].sum()

    df.principales_palabras_clave = df.principales_palabras_clave.str.replace(".", "", regex=True)
    df.principales_palabras_clave = df.principales_palabras_clave.str.replace("   "," ")
    df.principales_palabras_clave = df.principales_palabras_clave.str.replace("  "," ")

    df.porcentaje_depalabras_clave = df.porcentaje_depalabras_clave.str.replace('%', '')
    df.porcentaje_depalabras_clave = df.porcentaje_depalabras_clave.str.replace(',', '.')
    df.porcentaje_depalabras_clave = df.porcentaje_depalabras_clave.map(float)

    df.cluster = df.cluster.map(int)
    df = df.sort_values('cluster')
    df = df.reset_index(drop=True)

    return df
