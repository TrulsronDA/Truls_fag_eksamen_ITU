import pyspark.sql.functions as sf
import re
from pyspark.sql import DataFrame

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