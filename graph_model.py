"""graph model"""
import os
import sys
import uuid
import json
import logging
from datetime import datetime
import pandas as pd
import duckdb as dd


os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"logs/graph_model_{timestamp}.log"

logging.basicConfig(
	level=logging.INFO,
    format="%(levelname)s/%(processName)s %(asctime)s - %(message)s",
	datefmt="%Y-%m-%d %H:%M:%S",
	handlers=[
		logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
		logging.StreamHandler(sys.stdout)
	]
)


class DuckDBConn:
    """duckdb connector"""

    def __init__(self) -> None:
        """init db"""

        self.con  = dd.connect("bible_graph.db")


def json_to_df(filename: str) -> pd.DataFrame:
    """
	convert json to df
	"""

    try:
        with open(f"data/{filename}.json", "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        logging.error("file data/%s.json not found", filename)
        return pd.DataFrame()
    except json.JSONDecodeError:
        logging.error("Invalid JSON in data/%s.json", filename)
        return pd.DataFrame()

    df = pd.json_normalize(data, sep="")

    return df


def df_to_csv(filename=str, df=pd.DataFrame) -> None:
    """
	df to csv
	"""

    os.makedirs("data/csv", exist_ok=True)
    df.to_csv(f"data/csv/{filename}.csv", index=False)


def create_uuid(df: pd.DataFrame) -> pd.DataFrame:
    """
	create unique id for each item in df
	"""

    df.insert(loc=0, column="id", value=[uuid.uuid4().hex for _ in range(len(df))])

    if len(df) == df.id.nunique():
        logging.info("unuquess check passed!")
    else:
        logging.error("non-unique uuids found!")
    return df


def vertices(df: pd.DataFrame, vertex: str) -> None:
    """
    create node file
    """

    df = create_uuid(df=df)
    df.loc[:, "label"] = vertex

    df_to_csv(filename=f"{vertex}.vertices", df=df)
    logging.info("total %s vertices: %s", vertex, len(df))


def edges(from_df: pd.DataFrame, to_df: pd.DataFrame, edge: str) -> None:
    """
    create book -> version edges
    """

    edges_df = to_df.assign(key=1).merge(from_df.assign(key=1), on="key").drop("key", axis=1)
    edges_df = edges_df.rename(columns={"id_x": "from", "id_y": "to"})
    edges_df.loc[:, "label"] = edge
    edges_df = edges_df[["from", "to", "label"]]
    df_to_csv(filename=f"{edge}.edges", df=edges_df)
    logging.info("total %s edges: %s", edge, len(edges_df))


def main():
    """
	main loop
    """

    verses_df = json_to_df(filename="verses")
    books_df = json_to_df(filename="books")
    ver_df = json_to_df(filename="versions")
    regions_df = json_to_df(filename="regions")
    locations_df = json_to_df(filename="location_regions")

    vertices(df=books_df, vertex="books")
    vertices(df=ver_df, vertex="versions")
    vertices(df=verses_df, vertex="verses")
    vertices(df=regions_df, vertex="regions")
    vertices(df=locations_df, vertex="locations")

    edges(from_df=books_df, to_df=ver_df, edge="book_in_version")
    edges(from_df=verses_df, to_df=books_df, edge="verse_in_book")


if __name__ == "__main__":

    main()
