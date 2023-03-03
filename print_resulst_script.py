
import numpy as np
import pandas as pd
import re

# Load excel file
excel_file = pd.ExcelFile('Base de dados Case.xlsx')

# Load each sheet
populacao_estado_df = excel_file.parse('Populacao_Estado')
uf_df = excel_file.parse('De_para_UF')
pib_municipio_df = excel_file.parse('PIB_municipio')
uf_regiao = excel_file.parse('UF_Regiao')



""" 
Clean data before merging values.
"""
# Correct wrong UF acronym for "Mato Grosso"
uf_df.loc[10, 'UF'] = 'MT'
# Drop duplicates
uf_df.drop_duplicates(inplace=True)
# Cast "PIB" to integer, treat errors as null values
pib_municipio_df['PIB'] = pd.to_numeric(pib_municipio_df['PIB'], errors='coerce')
# Multiply PIB value to 1000
pib_municipio_df['PIB'] = pib_municipio_df['PIB'] * 1000
# Drop null values
pib_municipio_df.dropna(inplace=True)
# Create ID witj first two digits for Cod_Identificacao
pib_municipio_df['Cod_Identificacao_uf'] = pib_municipio_df['Cod_Identificacao'].apply(lambda x : int(x//1e5))
# Unique values for Cod_Identificacao in uf_df
pib_municipio_df['UF'] = pib_municipio_df['Municipio'].apply(lambda x : re.findall(r'\((.*)\)', x)[0])
# Extract UF acronym between () in "Municipio"
pib_municipio_df['UF'] = pib_municipio_df['Municipio'].apply(lambda x : re.findall(r'\((.*)\)', x)[0])
# Map values to "Regiao"
uf_regiao['Regiao'] = uf_regiao['Regiao'].map({
    'N' : 'Norte',
    'NE' : 'Nordeste',
    'S' : 'Sul',
    'SE' : 'Sudeste',
    'CO' : 'Centro-Oeste',
})



"""
Merge values from all sheets to group the following features:
- Estado
- UF
- Regiao
- Cod_Identificacao
- Populacao
- PIB
"""

# Starting with total values from populacao_estado_df
merged_df = (
    populacao_estado_df
    .query('fx_idade == "Total"')
    .drop(columns='fx_idade')
    .sort_values('Granularidade')
    .reset_index(drop=True)
)

# Extract Brasil data from "Granularidade"
brasil_population = merged_df[merged_df['Granularidade']=='Brasil']['Populacao'].values[0]
merged_df.dropna(inplace=True)

# Merge with uf_df to map UF acronyms
merged_df = pd.merge(
    left=merged_df,
    right=uf_df,
    on='Granularidade',
    how='left'
).dropna() # this dataframe also has Brasil data

# Group PIB values by UF
pib_municipio_grouped_df = (
    pib_municipio_df
    .groupby(['UF', 'Cod_Identificacao_uf'], as_index=False)
    ['PIB']
    .sum()
)

# Merge with groupped PIB values
merged_df = pd.merge(
    left=merged_df,
    right=pib_municipio_grouped_df,
    how='left',
    on='UF'
)

# Drop duplicated column
merged_df.drop(columns='Cod_Identificacao_uf', inplace=True)

# Merge with Regiao data
merged_df = pd.merge(
    left=merged_df,
    right=uf_regiao,
    how='left',
    left_on='Granularidade',
    right_on='Estado',
    left_index=False
)

# Reorder columns
merged_df = merged_df[[
    'Estado',
    'UF',
    'Regiao',
    'Cod_Identificacao',
    'Populacao',
    'PIB',
]]

# Add PIB_per_capita feature
merged_df['PIB_per_capita'] = merged_df['PIB'] / merged_df['Populacao']

# Calculate total PIB
pib_total = round(merged_df['PIB'].sum() / brasil_population, 2)

# Create dataframe for PIB per capita for each Macro region
results_df = (
    merged_df
    .groupby('Regiao', as_index=False)
    ['PIB_per_capita']
    .sum().round(2)
    .sort_values('Regiao')
    .append({
        'PIB_per_capita' : pib_total,
        'Regiao' : 'Total',
    }, ignore_index=True)
)

# Print results
print(results_df.to_string(index=False))

# Export result to csv
# results_df.to_csv('data/results.csv', index=False)
