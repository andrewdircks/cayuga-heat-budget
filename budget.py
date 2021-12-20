'''
Transform the raw temperature profile data in `data.xlsx` to daily heat budget
calculations, and store in `budget.csv`.

Andrew Dircks (abd93@cornell.edu)
Bennett McCombe (bem94@cornell.edu)
'''

import numpy as np
import pandas as pd

raw_xlsx = 'data/data.xlsx'
heat_budget_csv = 'data/budget.csv'

years = [2006, 2007, 2008, 2009, 2010, 2011, 2016]
sheets = [str(y) + ' complete' for y in years]


def load_organize(df):
    df = df[['Date', 'Depth (M)', 'Temp (C)']]
    df = df.rename(columns={'Depth (M)': 'Depth', 'Temp (C)': 'Temp'})
    df = df[df['Depth'].notnull()]
    df = df[df['Depth'] != 'Depth']
    df['Date'] = df['Date'].ffill()
    return df


def in_celcius(df):
    f_to_c = lambda x: (x - 32) * 5/9

    def celciusify(x):
        if type(x) == str:
            try:
                temp_f = float(x[:-2])
            except:
                print(x)
            return f_to_c(temp_f)
        elif x > 30:
            return f_to_c(x)
        else: 
            return x

    # convert to celcius
    df['Temp'] = df['Temp'].apply(celciusify)
    return df


reduced_lengths = {
    (0, 10): 8.34,
    (10,20): 6.88,
    (20,30): 6.28,
    (30,40): 5.75,
    (40,50): 5.11,
    (50,60): 4.52,
    (60,70): 4.05
}

def adjusted_lengthify(x):
    for (l, _), v in reduced_lengths.items():
        if l + 1 in x:
            return float(v)
    return 1.0


def heat_budget(df):
    df['mod_temp'] = df['Temp'] - 4
    df['adjusted_lengths'] = df['Depth'].apply(adjusted_lengthify).astype('float64')
    df['surface_heat_released'] = df['mod_temp'] * df['adjusted_lengths'] * 1000
    df['joules_released'] = 4.184 * df['surface_heat_released']
    return df

    
def process(sheet_name, f=raw_xlsx):
    df = pd.read_excel(f, sheet_name=sheet_name)
    df = load_organize(df)
    df = in_celcius(df)
    df = df.groupby(['Date', pd.cut(df['Depth'], np.arange(0, 80, 10))]).agg({'Temp': ['mean']})
    df = heat_budget(df.reset_index())
    df = df.groupby(['Date']).sum()
    return df[['Date', 'surface_heat_released', 'joules_released']]


if __name__ == '__main__':
    first = True
    for sheet in sheets:
        year_df = process(sheet)
        if first:
            df = year_df
            first = False
        else:
            df = pd.concat([df, year_df])
    
    df.reset_index().to_csv(heat_budget_csv)