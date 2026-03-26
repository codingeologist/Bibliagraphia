"""
TypeDB Bible Data Loader
Loads JSON data into TypeDB database
"""

# Imports
import json
from typedb.driver import *


# Configuration
DATABASE_NAME = "bible_graph"
SCHEMA_FILE = "bible_schema.tql"


class BibleDataLoader:

    def load_data(self):
        """Open a TypeDB driver connection"""

        # Define the schema from file
        with open(SCHEMA_FILE, "r") as file:
          schema = file.read()

        with TypeDB.driver(TypeDB.DEFAULT_ADDRESS, Credentials("admin", "password"), DriverOptions(is_tls_enabled=False)) as driver:

            # Create Database
            driver.databases.create(f"{DATABASE_NAME}")
            database = driver.databases.get(f"{DATABASE_NAME}")

            # Set Options
            options = TransactionOptions(transaction_timeout_millis=10_000)

            with driver.transaction(database.name, TransactionType.SCHEMA, options) as tx:
                answer = tx.query(schema).resolve()
                if answer.is_ok():
                    print("Schema defined successfully")
                tx.commit()
            
            # Write Versions
            with open("data/versions.json") as file:
                versions = json.load(file)
            for version in versions:
                versions_query = f"""
                    insert $v isa version,
                    has version_code "{version["code"]}",
                    has version_name "{version["name"]}",
                    has version_full_name "{version["full_name"]}";
                """
                with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                    tx.query(versions_query)
                    tx.commit()
            print(f"Loaded {len(versions)} versions")


            # Write Books
            with open("data/books.json") as file:
                books = json.load(file)
            for book in books:
                books_query = f"""
                    insert $b isa book,
                    has book_code "{book["code"]}",
                    has testament "{book["testament"]}",
                    has vulgate_name "{book["vulgate"]}",
                    has rheims_name "{book["rheims"]}",
                    has kjv_name "{book["kjv"]}",
                    has note "{book["note"]}";
                """
                with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                    tx.query(books_query)
                    tx.commit()
            print(f"Loaded {len(books)} books")


            # Write Verses
            with open("data/verses.json") as file:
                verses = json.load(file)
            for verse in verses:
                verses_query = f"""
                    insert $v isa verse,
                    has version_code "{verse["version_code"]}",
                    has version_name "{verse["version"]}",
                    has book_code "{verse["book_code"]}",
                    has chapter {verse["chapter"]},
                    has verse_number {verse["verse"]},
                    has text "{verse["text"].replace('"', '\"')}";
                """
                with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                    tx.query(verses_query)
                    tx.commit()
            print(f"Loaded {len(verses)} verses")


            # Write Locations
            with open("data/location_regions.json") as file:
                locations = json.load(file)
            for location in locations:
                locations_query = f"""
                    insert $l isa location,
                    has primary_name "{location["primary_name"]}",
                    has secondary_name "{location["secondary_name"]}",
                    has region_name "{location["region"]}",
                    has book_code "{location["book_code"]}",
                    has chapter {location["chapter"]},
                    has verse_number {location["verse"]},
                    has latitude {location["latitude"]},
                    has longitude {location["longitude"]};
                """
                with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                    tx.query(locations_query)
                    tx.commit()
            print(f"Loaded {len(locations)} locations")


            # Write Regions
            with open("data/regions.json") as file:
                regions = json.load(file)
            for region in regions:
                regions_query = f"""
                    insert $r isa region,
                    has region_name "{region["name"]}",
                    has keywords "{region["keywords"]}",
                    has description "{region["description"].replace('"', '\"')}";
                """
                with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                    tx.query(regions_query)
                    tx.commit()
            print(f"Loaded {len(regions)} regions")
            
            
            # Create verse_in_book relationships
            v_b_query = """
            match
                $v isa verse, has book_code $book_code, has verse_number $verse_number;
                $b isa book, has book_code $book_code;
            insert
                (verse_role: $v, book_role: $b) isa verse_in_book;
            """
            with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                tx.query(v_b_query)
                tx.commit()
            print("verse in book relation added!")


            # Create verse_in_version relationships
            v_ver_query = """
            match
                $v isa verse, has version_code $version_code, has verse_number $verse_number;
                $ver isa version, has version_code $version_code;
            insert
                (verse_role: $v, version_role: $ver) isa verse_in_version;
            """
            with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                tx.query(v_ver_query)
                tx.commit()
            print("verse in version relation added!")


            # Create location_in_verse relationships
            loc_v_query = """
            match
                $l isa location, has book_code $book_code, has chapter $chapter, has verse $verse, has region_name, $region_name;
                $v isa verse, has book_code $book_code, has chapter $chapter, has verse_number $verse;
            insert
                (location_role: $l, verse_role: $v) isa location_in_verse;
            """
            with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                tx.query(loc_v_query)
                tx.commit()
            print("location in verse relationadded!")


            # Create location_in_region relationships
            loc_r_query = """
            match
                $l isa location, has region_name $region_name;
                $r isa region, has name $region_name;
            insert
                (location_role: $l, region_role: $r) isa location_in_region;
            """
            with driver.transaction(database.name, TransactionType.WRITE, options) as tx:
                tx.query(loc_r_query)
                tx.commit()
            print("location in region relation added!")


if __name__ == "__main__":
    BibleDataLoader().load_data()
    print("Data loading complete!")