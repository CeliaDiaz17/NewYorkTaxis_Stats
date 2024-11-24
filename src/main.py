import json
import os
import random
from data_processing import load_and_clean_data, calculate_metrics, update_metrics
from chunk_gen import generate_chunks

'''
Modulo principal del programa. Se encarga de cargar el dataset principal, calcular las metricas y guardarlas en un archivo. 
Ademas, simula la llegada de nuevos datos en forma de chunks y actualiza las metricas en el archivo.
'''

ORIGINAL_METRICS_FILE = '../data/processed/20241123_yellow_taxi_kpis.json'
UPDATED_METRICS_FILE = '../data/processed/20241123_yellow_taxi_kpis_updated.json'

def analyce_main_dataset(main_dataset_path):
    print("Calculating data for the main dataset")
    
    df = load_and_clean_data(main_dataset_path)
    metrics = calculate_metrics(df)
    metrics['row_count'] = len(df)
    
    with open(ORIGINAL_METRICS_FILE, 'w') as f:
        json.dump(metrics, f)
    print(f"Metrics updated and saved: {ORIGINAL_METRICS_FILE}")
    
    with open(UPDATED_METRICS_FILE, 'w') as f:
        json.dump(metrics, f)
        
    
def process_new_chunk(chunk_path):
    if os.path.exists(UPDATED_METRICS_FILE):
        with open(UPDATED_METRICS_FILE, 'r') as f:
            cumulative_metrics = json.load(f)
    else:
        print("No metrics found. Please, analyze the main dataset first.")
        return
    
    print(f"Processing chunk: {chunk_path}")
    df = load_and_clean_data(chunk_path)
    new_metrics = calculate_metrics(df)
    new_row_count = len(df)
    
    cumulative_metrics = update_metrics(cumulative_metrics, new_metrics, new_row_count)
    
    with open(UPDATED_METRICS_FILE, 'w') as f:
        json.dump(cumulative_metrics, f)
    print(f"Metrics updated and saved: {UPDATED_METRICS_FILE}")

def main():
    main_dataset_path = '../data/raw/yellow_tripdata_2023-09.parquet'
    analyce_main_dataset(main_dataset_path)
    
    forchunk_dataset_path = '../data/raw/yellow_tripdata_2023-10.parquet'
    
    generate_chunks(forchunk_dataset_path, '../data/raw/chunks/')
    chunk_folder = '../data/raw/chunks'
    chunk_files =  [f for f in os.listdir(chunk_folder) if f.endswith('.parquet')]
    
    #se decide escoger un chunk aleatorio para simular una nueva llegada de datos al sistema
    if chunk_files:
        random_chunk = random.choice(chunk_files)
        random_chunk_path = os.path.join(chunk_folder, random_chunk)
        
        process_new_chunk(random_chunk_path)
        print("New data processed")
    else:
        print("No chunks available to process.")
    
    print("All done!")

if __name__ == "__main__":
    main()
