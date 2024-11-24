# NewYorkTaxis_Stats
My implementation for solving the NIQ proposed exercise of computing a set of metrics from the available New York taxi trip dataset.

## My proposal
In this repository you'll find my proposal for solving the homework proposed. 

## Folders 
- **Data**: This folder contains both processed and raw data. Once the chunks are created by the system, they will be stored in data/raw/chunks directory. Directory data/raw holds the datasets used for the implementation. One dataset is used as main dataset, and the second one is chunked in order to simulate new data income. The obtained .json metrics are stored in data/processed folder.

- **src**: Source code modules can be found in this folder.

### Modules and functionalities
The solution is composed by 3 main modules:
- **Data processing**: This module has every function dedicated to data loading, cleaning and needed calculations to generate the metrics.
- **Chunk generator**: This script takes as an input one specific .parquet file and outputs the file in chunks in an specific directory. This chunks simulate the arrival of new data.
- **Main**: Main module where everything is integrated.

Specific comments can be found inside the code.


## Instalation 
1. Clone this repository
    ``` bash
    git clone https://github.com/CeliaDiaz17/NewYorkTaxis_Stats

2. Navigate to project directory

3. Install virtual environment (not needed, but recommended)
    ```bash
    python3 -m venv myenv
    source myenv/bin/activate

4. Install the required dependencies

    ```bash
    pip install pandas
    pip install numpy
    pip install pyarrow 18.0.0

4. Or use the provided requirements.txt 

    ```bash
    pip install -r requirements.txt

5. Execute main module

    ```bash 
    python3 main.py

## Notes
- After executing the main module, two .json files will be generated inside the data/processed directory. The purpose of this implementation is to allow revisors to observe how the metrics change before and after the new data arrives.

- Datasets used in this project are .parquet files, not .csv. This is why python module 'pyarrow' needs to be installed. 

Datasets from: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page






