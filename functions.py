import pyspark.sql.functions as sf
import re
import os
from pyspark.sql import DataFrame
from databricks.sdk.runtime import *

def to_snake_case(column_name: str) -> str:
    """
    Konverterer et kolonnenavn til snake_case.

    Erstatter alle ikke-alfanumeriske tegn med understrek, 
    håndterer camelCase og PascalCase til snake_case, 
    og returnerer navnet med små bokstaver uten ledende eller 
    etterfølgende understrek.

    Args:
        column_name (str): Kolonnenavn som skal konverteres.

    Returns:
        str: Kolonnenavnet i snake_case-format.
    """
    # erstatte alle non alphabetisk tegn med understrek
    column_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)

    # Håndtere cameCase eller PascalCase til snake_case
    column_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', column_name)
    column_name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', column_name)

    # Fjern ledende/liggende understreker og skriv med små bokstaver
    return column_name.strip('_').lower()

def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    """
    Returnerer en kopi av DataFrame der alle kolonnenavn er konvertert til snake_case.

    Bruker funksjonen to_snake_case på alle kolonnenavn i DataFrame.
    Funksjonen er laget for PySpark DataFrames.

    Args:
        df (DataFrame): En PySpark DataFrame med originale kolonnenavn.

    Returns:
        DataFrame: En ny DataFrame med kolonnenavn i snake_case-format.
    """
    new_columns = [to_snake_case(col) for col in df.columns]
    return df.toDF(*new_columns)


def save_df_as_delta_and_csv(
    df: DataFrame,
    base_path: str,
    name: str,
    mode: str
):
    """
    Lagrer en Pyspark DataFrame som Delta og CSV med ordentlig struktur.
    
    Parameters:
    - df (DataFrame): Pyspark Dataframe til lagring
    - base_path (str): base directory path (f.eks. "/Volumes/dev_truls/default/data_files")
    - name (str): navnet til datasettet (som skal brukes for directory og filnavn)
    - mode (str): skriver mode, f.eks. "overwrite" or "append"
    """

    # Create Delta directory
    delta_path = f"{base_path}/{name}"
    df.write.format("delta").mode(mode).option("mergeSchema", "true").save(delta_path)
    
    # Convert to Pandas and create CSV string
    pandas_df = df.toPandas()
    csv_string = pandas_df.to_csv(index=False, header=True)
    
    # Define full CSV path
    csv_path = f"{base_path}/{name}.csv"
    
    # Write CSV file to destination
    dbutils.fs.put(csv_path, csv_string, True)