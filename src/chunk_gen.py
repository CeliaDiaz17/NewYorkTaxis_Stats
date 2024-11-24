import pandas as pd
import os

'''
El siguiente metodo se encarga de dividir un archivo en diferentes chunks
de tamaño especifico y guardarlos en un directorio. 
Estos chunks seran usados mas tarde en 'main.py' para simular la llegada de nuevos datos al sistema.
'''

def generate_chunks(input_file, output_dir, chunk_size=10000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #cargar todo el archivo en un dataframe
    df = pd.read_parquet(input_file)

    #dividirlo en diferentes chunks
    total_rows = len(df)
    for i in range(0, total_rows, chunk_size): #genera secuencias de 0 hasta total_rows con saltos de chunk_size
        chunk = df.iloc[i:i + chunk_size] #extrae un subconjunto de filas desde la i hasta i+chunk_size
        chunk_file = os.path.join(output_dir, f"chunk_{i // chunk_size + 1}.parquet")
        chunk.to_parquet(chunk_file)
        print(f"Generated: {chunk_file}")

#main para pruebas
if __name__ == '__main__':
    input_file = '../data/raw/yellow_tripdata_2023-10.parquet'
    output_dir = '../data/raw/chunks/'
    generate_chunks(input_file, output_dir)
