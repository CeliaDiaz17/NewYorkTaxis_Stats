import pandas as pd

def load_and_clean_data(file_path):
    df = pd.read_parquet(file_path)
    
    #quitamos nulos y se eliminan las filas que tienen distancia de viaje 0 para evitar divisiones por 0
    df = df.dropna(subset=['trip_distance', 'total_amount', 'payment_type', 'tip_amount', 'extra'])
    df = df[df['trip_distance'] > 0]
    
    return df

def calculate_metrics(df):
    #average price per mile
    df['price_per_mile'] = df['total_amount'] / df['trip_distance']
    
    df['price_per_mile'] = df['price_per_mile'].replace([float('inf'), float('-inf')], 0).fillna(0)
    average_price_per_mile = df['price_per_mile'].mean()

    #distribution of payment types
    payment_type_distribution = df['payment_type'].value_counts(normalize=True).to_dict()

    #custom indicator: (amount of tip + extra payment) / trip distance
    df['custom_indicator'] = (df['tip_amount'] + df['extra']) / df['trip_distance']
    
    df['custom_indicator'] = df['custom_indicator'].replace([float('inf'), float('-inf')], 0).fillna(0)
    custom_indicator = df['custom_indicator'].mean()
    
    metrics_dict = {
        'average_price_per_mile': average_price_per_mile,
        'payment_type_distribution': payment_type_distribution,
        'custom_indicator': custom_indicator 
        }
    
    return metrics_dict




