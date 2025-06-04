# World Bank Indicators – Fagprøveprosjekt

## Om prosjektet
Dette prosjektet er en del av min fagprøve i IT-utviklerfaget.
Målet er å hente inn, transformere og analysere nøkkelindikatorer fra World Bank for ulike land og år, og gjøre dataene klare for videre analyse og visualisering.

Oppdateringsdato: 4 Juni 2025, klokkeslett: 10:30

### Lisens
Dette prosjektet er til IT-utviklerfaget fagprøvebruk

## Innhold

### Data innhold
Dette prosjektet inneholder utdanningsutgifter, total (% av BNP), forventet levealder fra fødsel, total (år), offentlige utgifter, BNP per innbygger data fra World Bank Group.
- Kilde for World Bank data: https://data.worldbank.org/indicator?tab=featured

### Krav og avhengigheter
* Python 3.x
* pandas
* Pyspark (Apache Spark)
* Azure Databricks (eller jupyter Notebook for lokal kjøring) 

### ETL-prosess:

Hente inn data fra World Bank (CSV-filer)

Rense og transformere datasett (fjerne metadata, endre datatype, snake_case, etc.)

Slå sammen indikatorer til én strukturert DataFrame

Eksportere datasettet som delta-tabell og csv-fil

### Kildekode:

* functions.py: Egenutviklede funksjoner for datarensing, navngivning og eksport
* World_Bank_Data_project.ipynb: Hovednotebook med hele arbeidsprosessen

## Hvordan bruke prosjektet

1. Last ned eller klon repoet
2. Kjør notebooken (World_Bank_Data_project.ipynb) i Databricks eller Jupyter

3. Funksjoner for eksport:

* Bruk funksjonen `save_df_as_delta_and_csv()` for å lagre datasettet som Delta-tabell og CSV-fil

4. Videre analyse:

* Ferdige CSV-filer kan lastes inn i Power BI eller andre analyseverktøy

## Struktur

/
├── functions.py
├── World_Bank_Data_project.ipynb
├── data_files/
│   ├── world_bank_indicators.csv
│   └── ... (andre datasett)
├── README.md

## Viktige funksjoner

* `rename_columns_to_snake_case(df)`
Konverterer alle kolonnenavn til snake_case

* `save_df_as_delta_and_csv(df, base_path, name, mode)`
Lagrer DataFrame som Delta-tabell og CSV-fil

## Kontakt
For spørsmål, kontakt Truls Grude Rønning på e-post: truls.ronning@autostoresystem.com



