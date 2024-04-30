import numpy as np
import pandas as pd
import requests
import json
import openpyxl

def get_radon_data():
    kod_cast = np.array([])
    kod_obec = np.array([])
    radon = np.array([])
    for i in range(16):
        url = 'https://mapy.geology.cz/arcgis/rest/services/Geohazardy/radon_komplexni_informace/MapServer/0/query?where=objectid%3E' + str(0 + 1000*i) + '+and+objectid+%3C' + str(1000 + 1000*i) + '&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=objectid%2C+kod_obec%2C+kod_cast%2C+radon&returnGeometry=false&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=pjson'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data['features'])
            kod_cast = np.concatenate([kod_cast, df['attributes.kod_cast'].values])
            kod_obec = np.concatenate([kod_obec, df['attributes.kod_obec'].values])
            radon = np.concatenate([radon, df['attributes.radon'].values])
        else:
            print("Failed to retrieve data. Status code: ", response.status_code)
    np.save('radondata.npy', {'kod_cast': kod_cast,'kod_obec': kod_obec, 'radon': radon})
    return (kod_cast, radon)

def get_population(kod_cast):
    df = pd.read_csv('sldb2021_obyv_byt_cob_zsj.csv')
    # Filter the DataFrame to include only rows where uzemi_typ is "část obce" and ukaz_txt is "Počet obyvatel s obvyklým pobytem"
    df = df[df['uzemi_typ'] == 'část obce']
    df = df[df['ukaz_txt'] == 'Počet obyvatel s obvyklým pobytem']

    # Create an empty array to store population sizes
    population = np.array([])

    # Loop through the input kod_cast values
    for kod in kod_cast:
        # Find the population size corresponding to the kod_cast value
        population_size = df.loc[df['uzemi_kod'] == int(kod), 'hodnota'].values
        # If a population size is found, append it to the population array
        if len(population_size) > 0:
            population = np.append(population, population_size)
        # If no population size is found, append NaN
        else:
            population = np.append(population, np.nan)

    return population

def get_okres(kod_obec):
    df = pd.read_csv('VAZ0043_0101_CS.csv')
    okres = np.array([])
    for kod in kod_obec:
        okr = df.loc[df['chodnota1'] == int(kod), 'text2'].values
        if len(okr) > 0:
            okres = np.append(okres, okr)
        else:
            okres = np.append(okres, np.nan)
    return okres

def get_kraj(okresy):
    df = pd.read_csv('CiselnikOkresu_mod.csv', sep=';')
    kraje = np.array([])
    for okres in okresy:
        kraj = df.loc[df['Nazev_okresu'] == okres, 'Nazev_kraje'].values
        if len(kraj) > 0:
            kraje = np.append(kraje, kraj)
        else:
            kraje = np.append(kraje, np.nan)
    return kraje

def fill_table(kod_cast, radon, population, okres, kraj):

    table = pd.DataFrame(
        {
            "kod_cast": kod_cast,
            "radon": radon,
            "populace": population,
            "okres": okres,
            "kraj": kraj
        }
    )
    print(table)
    table.to_csv('table.csv', index=False)

def prepare_cancer_data():
    radon_table = pd.read_csv('table.csv')
    # Group by 'okres' and calculate the weighted average of radon index
    weighted_avg_radon = radon_table.groupby('okres').apply(lambda x: np.average(x['radon'], weights=x['populace'])).reset_index()

    # Rename the columns
    weighted_avg_radon.columns = ['okres', 'radon_average']
    weighted_avg_radon["vyskyt"] = np.nan

    # Load the Excel file
    df_excel = pd.read_excel('nor-ds-okresy-incidence-10lete.xlsx')
    print(df_excel)

    for row in range(37275,46590):
        if df_excel.iloc[row, 0] == '2017–2021' and df_excel.iloc[row, 3] == 'ZN průdušnice, průdušky a plíce (C33, C34)':
            weighted_avg_radon.loc[weighted_avg_radon['okres'] == df_excel.iloc[row, 2], 'vyskyt'] = df_excel.iloc[row, 12]

    # Print the result
    print(weighted_avg_radon)
    weighted_avg_radon.to_csv('results.csv', index=False)

def analyse_data():
    data = pd.read_csv('results_mod.csv')
    
    # Convert string values to numeric types
    data['radon_average'] = pd.to_numeric(data['radon_average'], errors='coerce')
    data['vyskyt'] = pd.to_numeric(data['vyskyt'], errors='coerce')
    
    # Check for NaN values after conversion
    radon_average_nan = data['radon_average'].isna().any()
    vyskyt_nan = data['vyskyt'].isna().any()
    
    print("NaN values in 'radon_average':", radon_average_nan)
    print("NaN values in 'vyskyt':", vyskyt_nan)
    
    # Calculate correlation matrix if there are no NaN values
    if not radon_average_nan and not vyskyt_nan:
        matrix = np.corrcoef(data['radon_average'], data['vyskyt'])
        print(matrix)
    else:
        print("NaN values detected. Cannot calculate correlation matrix.")

def main():
    #analyse_data()
    #prepare_cancer_data()
    data = np.load('radondata.npy', allow_pickle=True)
    kod_cast = data.item().get('kod_cast')
    kod_obec = data.item().get('kod_obec')
    radon = data.item().get('radon')
    population = get_population(kod_cast)
    okres = get_okres(kod_obec)
    kraje = get_kraj(okres)
    fill_table(kod_cast, radon, population, okres, kraje)

if __name__ == "__main__":
    main()
