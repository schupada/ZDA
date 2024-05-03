import numpy as np
import pandas as pd
import requests
import matplotlib.pyplot as plt
from scipy import stats

def get_radon_data():
    print("INFO: fetching radon data")
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
    print("INFO: saved radon data")
    return (kod_cast, radon)

def get_population(kod_cast):
    print("INFO: appending population")
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
    print("INFO: appending okresy")
    df = pd.read_csv('VAZ0043_0101_CS.csv')
    okresy = np.array([])
    for kod in kod_obec:
        okres = df.loc[df['chodnota1'] == int(kod), 'text2'].values
        if len(okres) > 0:
            # Special case for Praha and Rychnov nad Kněžnou
            if okres == "Praha":
                okresy = np.append(okresy, "Hlavní město Praha")
            elif okres == "Rychnov nad Kněžnou":
                okresy = np.append(okresy, "Rychnov n.Kněžnou")
            else:
                okresy = np.append(okresy, okres)
        else:
            okresy = np.append(okresy, np.nan)
    return okresy

def get_kraj(okresy):
    print("INFO: appending kraje")
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
    print("INFO: creating helper table")
    table = pd.DataFrame(
        {
            "kod_cast": kod_cast,
            "radon": radon,
            "populace": population,
            "okres": okres,
            "kraj": kraj
        }
    )
    table.to_csv('table.csv', index=False)
    print("INFO: helper table saved")

# Pandas z nejakeho duvodu potrebuje vlastni funkci na weighted average
def w_avg(df, values, weights):
    d = df[values]
    w = df[weights]
    return (d * w).sum() / w.sum()

def prepare_cancer_data_per_okres():
    print("INFO: creating okres table")
    radon_table = pd.read_csv('table.csv')
    weighted_avg_radon = radon_table.groupby('okres').apply(w_avg, 'radon', 'populace').reset_index()
    # Rename the columns
    weighted_avg_radon.columns = ['okres', 'radon_average']
    weighted_avg_radon["vyskyt"] = np.nan

    weighted_avg_radon.to_csv('weighted_avg_radon.csv', index=False)

    # Load the Excel file
    df_excel = pd.read_excel('nor-ds-okresy-incidence-10lete.xlsx')

    for row in range(37275,46590):
        if df_excel.iloc[row, 0] == '2017–2021' and df_excel.iloc[row, 3] == 'ZN průdušnice, průdušky a plíce (C33, C34)':
            weighted_avg_radon.loc[weighted_avg_radon['okres'] == df_excel.iloc[row, 2], 'vyskyt'] = df_excel.iloc[row, 12]

    weighted_avg_radon.to_csv('results_okresy.csv', index=False)

def prepare_cancer_data_per_kraj():
    print("INFO: creating kraj table")
    radon_table = pd.read_csv('table.csv')
    weighted_avg_radon = radon_table.groupby('kraj').apply(w_avg, 'radon', 'populace').reset_index()
    # Rename the columns
    weighted_avg_radon.columns = ['kraj', 'radon_average']
    weighted_avg_radon["vyskyt"] = np.nan
    weighted_avg_radon["particulate_matter"] = np.nan

    weighted_avg_radon.to_csv('weighted_avg_radon_kraje.csv', index=False)

    # Load the Excel file
    df_excel = pd.read_excel('nor-ds-kraje-incidence-10lete.xlsx')
    air_quality_excel = pd.read_excel('320198210308.xlsx')

    for row in range(43687,45383):
        if str(df_excel.iloc[row, 0]) == '2019' and df_excel.iloc[row, 2] == 'ZN průdušnice, průdušky a plíce (C33, C34)':
            weighted_avg_radon.loc[weighted_avg_radon['kraj'] == df_excel.iloc[row, 1], 'vyskyt'] = df_excel.iloc[row, 11]

    # manually add air quality data
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Hlavní město Praha', 'particulate_matter'] = air_quality_excel.iloc[10, 2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Jihomoravský kraj', 'particulate_matter'] = air_quality_excel.iloc[40,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Jihočeský kraj', 'particulate_matter'] = air_quality_excel.iloc[16,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Karlovarský kraj', 'particulate_matter'] = air_quality_excel.iloc[22,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Kraj Vysočina', 'particulate_matter'] = air_quality_excel.iloc[37,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Královéhradecký kraj', 'particulate_matter'] = air_quality_excel.iloc[31,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Liberecký kraj', 'particulate_matter'] = air_quality_excel.iloc[28,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Moravskoslezský kraj', 'particulate_matter'] = air_quality_excel.iloc[49,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Olomoucký kraj', 'particulate_matter'] = air_quality_excel.iloc[43,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Pardubický kraj', 'particulate_matter'] = air_quality_excel.iloc[34,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Plzeňský kraj', 'particulate_matter'] = air_quality_excel.iloc[19,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Středočeský kraj', 'particulate_matter'] = air_quality_excel.iloc[13,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Zlínský kraj', 'particulate_matter'] = air_quality_excel.iloc[46,2]
    weighted_avg_radon.loc[weighted_avg_radon['kraj'] == 'Ústecký kraj', 'particulate_matter'] = air_quality_excel.iloc[25,2]

    weighted_avg_radon.to_csv('results_kraje.csv', index=False)

def analyse_data():
    print("INFO: analysing data")
    print("OKRESY RESULTS")
    data = pd.read_csv('results_okresy.csv')
    # Convert string values to numeric types
    data['radon_average'] = pd.to_numeric(data['radon_average'], errors='coerce')
    data['vyskyt'] = pd.to_numeric(data['vyskyt'], errors='coerce')

     # Check for NaN values after conversion
    radon_average_nan = data['radon_average'].isna().any()
    vyskyt_nan = data['vyskyt'].isna().any()

    print("radon avg: ", data['radon_average'].mean())
    print("vyskyt avg: ", data['vyskyt'].mean())

    print("radon sd: ", data['radon_average'].std())
    print("vyskyt sd: ", data['vyskyt'].std())

    if not radon_average_nan and not vyskyt_nan:
        # Extract the columns as NumPy arrays
        radon_average = data['radon_average'].to_numpy()
        vyskyt = data['vyskyt'].to_numpy()
        
        # Compute the correlation matrix
        matrix = np.corrcoef([radon_average, vyskyt])
        print(matrix)
    else:
        print("NaN values detected. Cannot calculate correlation matrix.")

    print("KRAJE RESULTS")

    data = pd.read_csv('results_kraje.csv')
    
    # Convert string values to numeric types
    data['radon_average'] = pd.to_numeric(data['radon_average'], errors='coerce')
    data['vyskyt'] = pd.to_numeric(data['vyskyt'], errors='coerce')
    data['particulate_matter'] = pd.to_numeric(data['particulate_matter'], errors='coerce')
    
    # Check for NaN values after conversion
    radon_average_nan = data['radon_average'].isna().any()
    vyskyt_nan = data['vyskyt'].isna().any()
    particulate_matter_nan = data['particulate_matter'].isna().any()

    print("radon avg: ", data['radon_average'].mean())
    print("vyskyt avg: ", data['vyskyt'].mean())
    print("particulate_matter avg: ", data['particulate_matter'].mean())

    print("radon sd: ", data['radon_average'].std())
    print("vyskyt sd: ", data['vyskyt'].std())
    print("particulate_matter sd: ", data['particulate_matter'].std())
    
    # Calculate correlation matrix if there are no NaN values
    if not radon_average_nan and not vyskyt_nan and not particulate_matter_nan:
        # Extract the columns as NumPy arrays
        radon_average = data['radon_average'].to_numpy()
        vyskyt = data['vyskyt'].to_numpy()
        particulate_matter = data['particulate_matter'].to_numpy()
        
        # Compute the correlation matrix
        matrix = np.corrcoef([radon_average, vyskyt, particulate_matter])
        print(matrix)
    else:
        print("NaN values detected. Cannot calculate correlation matrix.")

def calculate_regression_okresy():
    print("OKRESY REGRESSION")
    data = pd.read_csv('results_okresy.csv')
    # Convert string values to numeric types
    data['radon_average'] = pd.to_numeric(data['radon_average'], errors='coerce')
    data['vyskyt'] = pd.to_numeric(data['vyskyt'], errors='coerce')

    x = data['radon_average']
    y = data['vyskyt']

    slope, intercept, r, p, std_err = stats.linregress(x, y)

    print("slope: ", slope)
    print("intercept", intercept)
    print("p: ", p)

    def myfunc(x):
        return slope * x + intercept

    mymodel = list(map(myfunc, x))

    plt.scatter(x, y)
    plt.plot(x, mymodel)
    plt.xlabel("radonový index")
    plt.ylabel("incidence novotvarů plic na 100 000 obyvatel")
    #plt.legend()
    plt.savefig('novotvary_dle_radonu_okresy.pdf')
    plt.savefig('novotvary_dle_radonu_okresy.png')
    plt.show()

def calculate_regression_kraje():
    print("KRAJE REGRESSION")
    data = pd.read_csv('results_kraje.csv')
    # Convert string values to numeric types
    data['radon_average'] = pd.to_numeric(data['radon_average'], errors='coerce')
    data['vyskyt'] = pd.to_numeric(data['vyskyt'], errors='coerce')
    data['particulate_matter'] = pd.to_numeric(data['particulate_matter'], errors='coerce')

    x = data['radon_average']
    y = data['vyskyt']

    slope, intercept, r, p, std_err = stats.linregress(x, y)

    print("slope: ", slope)
    print("intercept", intercept)
    print("p: ", p)

    def myfunc(x):
        return slope * x + intercept

    mymodel = list(map(myfunc, x))

    plt.scatter(x, y, color="red")
    plt.plot(x, mymodel, color="red")
    plt.xlabel("radonový index")
    plt.ylabel("incidence novotvarů plic na 100 000 obyvatel")
    plt.savefig('novotvary_dle_radonu_kraje.pdf')
    plt.savefig('novotvary_dle_radonu_kraje.png')
    plt.show()

    x = data['particulate_matter']
    slope, intercept, r, p, std_err = stats.linregress(x, y)
    mymodel = list(map(myfunc, x))

    plt.scatter(x, y, color="green")
    plt.plot(x, mymodel, color="green")
    plt.xlabel("pevné částice [t/km2]")
    plt.ylabel("incidence novotvarů plic na 100 000 obyvatel")
    plt.savefig('novotvary_dle_pevnych_castic_kraje.pdf')
    plt.savefig('novotvary_dle_pevnych_castic_kraje.png')
    plt.show()

def main():
    get_radon_data()
    data = np.load('radondata.npy', allow_pickle=True)
    kod_cast = data.item().get('kod_cast')
    kod_obec = data.item().get('kod_obec')
    radon = data.item().get('radon')
    population = get_population(kod_cast)
    okres = get_okres(kod_obec)
    kraje = get_kraj(okres)
    fill_table(kod_cast, radon, population, okres, kraje)
    prepare_cancer_data_per_okres()
    prepare_cancer_data_per_kraj()
    analyse_data()
    calculate_regression_okresy()
    calculate_regression_kraje()

if __name__ == "__main__":
    main()
