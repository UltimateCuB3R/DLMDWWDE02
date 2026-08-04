from dataclasses import dataclass
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd

DB_DEFINITION = Path('data/data_definitions.xml')


@dataclass(frozen=True)
class PdTable:
    table_name: str
    df: pd.DataFrame


def load_csv_into_table(file_path: str, table_name: str) -> PdTable:
    df = _load_csv_into_pandas(Path(file_path), table_name)
    df_cleaned = _clean_df(df, table_name)
    return PdTable(table_name=table_name, df=df_cleaned)


def _load_csv_into_pandas(file_path: Path, table_name: str, encoding: str = "utf-8"):
    """
    Test helper to load a CSV file into a pandas DataFrame.
    """

    if not file_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")
    if not DB_DEFINITION.exists():
        raise FileNotFoundError(f"Database definition file does not exist: {DB_DEFINITION}")

    dtypes = load_dtypes_for_table(table_name)
    return pd.read_csv(
        file_path,
        encoding=encoding,
        dtype=dtypes,
        na_values=["--"],
        keep_default_na=True,
    )


def load_dtypes_for_table(table_name: str) -> dict[str, str]:
    tree = ET.parse(DB_DEFINITION)
    root = tree.getroot()
    table_node = root.find(f"./DATABASE/TABLE[@name='{table_name}']")

    if table_node is None:
        raise ValueError(f"Table '{table_name}' not found in database definition file: {DB_DEFINITION}")

    dtypes: dict[str, str] = {}
    for column in table_node.findall("column"):
        column_name = column.attrib.get("name")
        column_dtype = column.attrib.get("dtype")
        if not column_name or not column_dtype:
            continue
        dtypes[column_name] = _to_pandas_dtype(column_dtype)
    return dtypes


def _to_pandas_dtype(xml_dtype: str) -> str:
    if xml_dtype == "int64":
        return "Int64"
    return xml_dtype


def _clean_df(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    match table_name:
        case 'games':
            pass
        case 'events':
            pass
        case 'pitches':
            df = _drop_non_integer_mph(df)  # column MPH has non-numeric values in CSV
    return df


def _drop_non_integer_mph(df: pd.DataFrame) -> pd.DataFrame:
    if "MPH" not in df.columns:
        return df

    mph_numeric = pd.to_numeric(df["MPH"], errors="coerce")
    integer_mask = mph_numeric.notna() & (mph_numeric % 1 == 0)
    cleaned = df.loc[integer_mask].copy()
    cleaned["MPH"] = mph_numeric.loc[integer_mask].astype("Int64")
    return cleaned
