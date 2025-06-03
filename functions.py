import pyspark.sql.functions as sf
import re
from pyspark.sql import DataFrame

def to_snake_case(column_name: str) -> str:
    # erstatte alle non alphabetisk tegn med understrek
    column_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)

    # Håndtere cameCase eller PascalCase til snake_case
    column_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', column_name)
    column_name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', column_name)

    # Fjern ledende/liggende understreker og skriv med små bokstaver
    return column_name.strip('_').lower()

def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    new_coulmns = [to_snake_case(col) for col in df.columns]
    return df.toDF(*new_coulmns)