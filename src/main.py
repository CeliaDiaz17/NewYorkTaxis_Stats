from data_processing import load_and_clean_data, calculate_metrics
from json_writer import save_metrics

def main():
    input_path = '../data/raw/yellow_tripdata_2023-09.parquet'
    
    df = load_and_clean_data(input_path)
    metrics = calculate_metrics(df)
    
    save_metrics(metrics)
    
if __name__ == '__main__':
    main()