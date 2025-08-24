from pathlib import Path
import pandas as pd


def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def read_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    ensure_parent(path)
    df.to_csv(path, index=False)


def write_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    ensure_parent(path)
    df.to_parquet(path, index=False)
