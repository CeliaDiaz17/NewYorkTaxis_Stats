import json
from datetime import datetime
import data_processing

def save_metrics(metrics):
    output_path = '../data/processed'
    date_str = datetime.now().strftime('%Y%m%d')
    file_name = f"{output_path}/{date_str}_yellow_taxi_metrics.json"
    
    with open(file_name, 'w') as f:
        json.dump(metrics, f)
    print(f"Metrics saved to {file_name}")