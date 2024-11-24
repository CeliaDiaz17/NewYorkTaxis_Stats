import pandas as pd

'''
En este modulo se encuentran los metodos necesarios para todo lo referido al procesamiento de datos.
'''

def load_and_clean_data(file_path):
    df = pd.read_parquet(file_path)
    
    #quitamos nulos y se eliminan las filas que tienen distancia de viaje 0 para evitar divisiones por 0
    df = df.dropna(subset=['trip_distance', 'total_amount', 'payment_type', 'tip_amount', 'extra'])
    df = df[df['trip_distance'] > 0]
    
    return df

#se aplican las metricas propuestas
def calculate_metrics(df):
    #average price per mile
    df['price_per_mile'] = df['total_amount'] / df['trip_distance']
    
    df['price_per_mile'] = df['price_per_mile'].replace([float('inf'), float('-inf')], 0).fillna(0) #reemplaza inf y -inf por 0 y los nulos por 0
    average_price_per_mile = df['price_per_mile'].mean()

    #distribution of payment types
    payment_type_distribution = df['payment_type'].value_counts().to_dict()
    
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

def update_metrics(existing_metrics, new_metrics, new_row_count):
    #media del precio por milla actualizada
    existing_avg_price_per_mile = existing_metrics['average_price_per_mile']
    existing_row_count = existing_metrics.get('row_count', 0)  # Estimar conteo inicial, si fuera necesario
    total_row_count = existing_row_count + new_row_count

    updated_avg_price_per_mile = (
        (existing_avg_price_per_mile * existing_row_count) + (new_metrics['average_price_per_mile'] * new_row_count)
    ) / total_row_count

    #se aseguran los tipos para que no de problemas la actualizacion
    new_payment_distribution = {
        str(payment_type): count #aseguro que las claves de diccionario sean strings
        for payment_type, count in new_metrics.get('payment_type_distribution', {}).items()
    }
    updated_payment_type_distribution = {
        str(payment_type): count
        for payment_type, count in existing_metrics.get('payment_type_distribution', {}).items()
    }

    #tipo de pago actualizado
    for payment_type, new_count in new_payment_distribution.items(): #se recorre cada tipo de pago
        updated_payment_type_distribution[payment_type] = (updated_payment_type_distribution.get(payment_type, 0) + new_count) #se actualiza

    #custom indicator actualizado
    existing_custom_indicator = existing_metrics['custom_indicator']
    updated_custom_indicator = ((existing_custom_indicator * existing_row_count) + (new_metrics['custom_indicator'] * new_row_count)) / total_row_count


    updated_metrics = {
        'average_price_per_mile': updated_avg_price_per_mile,
        'payment_type_distribution': updated_payment_type_distribution,
        'custom_indicator': updated_custom_indicator,
        'row_count': total_row_count  #total acumulado por si hace falta luego
    }

    return updated_metrics





