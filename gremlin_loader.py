import os
import sys
import csv
import logging
import multiprocessing
from datetime import datetime
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import T
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection


os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"logs/graph_load_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s/%(processName)s] %(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)


def get_traversal():
    conn = DriverRemoteConnection("ws://localhost:8182/gremlin", "g")
    g = traversal().withRemote(conn)
    return g, conn

def clear_graph():
    g, conn = get_traversal()
    try:
        # Try modern drop() first, fallback to manual removal for older TinkerPop
        try:
            g.V().drop().iterate()
        except:
            # For older JanusGraph: iterate and remove individually
            for v in g.V().toList():
                v.remove()
        logging.info("Cleared graph vertices and edges")
    except Exception as ex:
        logging.error(f"Error clearing graph: {ex}", exc_info=True)
    finally:
        conn.close()


def load_vertices(batch):
    g, conn = get_traversal()
    try:
        for row in batch:
            # Start with the label and id
            query = g.addV(row["label"]).property(T.id, row["id"])

            # Dynamically add all other properties
            for key, value in row.items():
                if key not in ["id", "label"]:
                    query = query.property(key, value)

            query.iterate()
        logging.info(f"Processed batch of {len(batch)} vertices")
    except Exception as ex:
        logging.error(f"Error processing vertex batch: {ex}", exc_info=True)
    finally:
        conn.close()


def load_edges(batch):
    g, conn = get_traversal()
    try:
        for row in batch:
            g.V().hasId(row["from"]).as_("src") \
                .V().hasId(row["to"]).as_("tgt") \
                .addE(row["label"]).from_("src").to("tgt").iterate()
        logging.info(f"Processed batch of {len(batch)} edges")
    except Exception as ex:
        logging.error(f"Error processing edge batch: {ex}", exc_info=True)
    finally:
        conn.close()


def read_csv_chunk(file_path: str, chunk_size=1000):
    with open(file=file_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch


def get_task_func(mode):
    if mode == "vertices":
        return load_vertices
    else:
        return load_edges


def parallel_load(csv_file: str, num_processes: int, chunk_size: int, mode="vertices"):
    pool = multiprocessing.Pool(processes=num_processes)
    task_func = get_task_func(mode)

    logging.info(f"Starting parallel load with {num_processes} processes...")
    batch_count = 0

    try:
        batches = list(read_csv_chunk(file_path=csv_file, chunk_size=chunk_size))
        batch_count = len(batches)
        logging.info(f"Prepared {batch_count} batches from {csv_file}")
        pool.map(task_func, batches)
    except Exception as ex:
        logging.error(f"Error during parallel load: {ex}", exc_info=True)
    finally:
        pool.close()
        pool.join()
        logging.info(f"Completed processing {batch_count} batches")


def main():
    logging.info("Starting new GraphDB load...")
    clear_graph()

    vertices = ["versions", "books", "locations", "regions", "verses"]
    edges = ["book_in_version", "verse_in_book"]

    for vert in vertices:
        parallel_load(
            csv_file=f"data/csv/{vert}_vertices.csv",
            num_processes=16,
            chunk_size=1000,
            mode="vertices"
        )
        logging.info(f"{vert} loaded")
    
    for edge in edges:
        parallel_load(
            csv_file=f"data/csv/{edge}.edges.csv",
            num_processes=16,
            chunk_size=1000,
            mode="edges"
        )
        logging.info(f"{edge} loaded")

    logging.info("Process completed!")


if __name__ == "__main__":

    main()
