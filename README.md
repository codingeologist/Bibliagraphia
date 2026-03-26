# Bibliagraphia

The Holy Bible mapped as a graph database built with TypeDB enabling semantic searches across Bible verses and versions with geographical locations and region mapping.

## Overview

This project creates a comprehensive graph database of Bible verses with:
- Multiple translations (Vulgate, Douay-Rheims, King James)
- Geographical location references
- Regional information
- Full book and chapter structure

## Data Sources

The database is built from JSON files in the `/data` directory:

- `books.json` - Bible books with translation names
- `versions.json` - Bible version information
- `verses.json` - Complete verse text (33M+ records)
- `location_regions.json` - Geographical locations with coordinates
- `regions.json` - Regional descriptions and keywords

## Schema Design

### Entities

| Entity        | Attributes                                                                                |
|---------------|-------------------------------------------------------------------------------------------|
| **Book**      | code, testament, vulgate-name, rheims-name, kjv-name, note                                |
| **Version**   | code, name, full-name                                                                     |
| **Verse**     | version-code, version-name, book-code, book-name, chapter, verse-number, text             |
| **Location**  | primary-name, secondary-name, region-name, book-code, chapter, verse, latitude, longitude |
| **Region**    | name, keywords, description                                                               |

### Relations

- `verse-in-book`: Connects verses to their books
- `verse-in-version`: Connects verses to Bible versions
- `location-mentioned-in-verse`: Links locations to specific verses
- `location-in-region`: Associates locations with geographical regions

## Installation

### Prerequisites

- Python 3.7+
- TypeDB server >3.8 (running on localhost:1729)

### Setup

```bash
# Clone the repository
git clone https://github.com/your-repo/bible-graph.git
cd bible-graph

# Install dependencies
pip install -r requirements.txt

# Ensure TypeDB server is running
docker run -d -p 1729:1729 -v typedb-data:/srv/typedb/data typedb/typedb:latest
```

## Data Loading

Run the data loader script:

```bash
python load_data.py
```

**Note**: This may take awhile to complete

## Query Examples

### Basic Queries

```typeql
# Find all books in the Old Testament
match
  $b isa book, has testament "Old Testament";
fetch $b;

# Get all verses in Genesis chapter 1
match 
  $v isa verse, has book-code "GEN", has chapter 1;
  $b isa book, has code "GEN";
  verse-in-book($v, $b);
fetch $v, $b;
```

### Location Queries

```typeql
# Find all locations mentioned in the New Testament
match
  $l isa location;
  $v isa verse;
  $b isa book, has testament "New Testament";
  verse-in-book($v, $b);
  location-mentioned-in-verse($l, $v);
fetch $l;

# Get locations in Syria with their coordinates
match
  $l isa location, has region-name "Syria";
fetch $l, $l.latitude, $l.longitude;
```

### Version-Specific Queries

```typeql
# Compare the same verse across different versions
match
  $v isa verse, has book-code "JOH", has chapter 3, has verse-number 16;
  $ver isa version;
  verse-in-version($v, $ver);
fetch $v, $ver;

# Find all King James Version verses mentioning Jerusalem
match
  $v isa verse;
  $ver isa version, has code "KJV";
  $l isa location, has primary-name "Jerusalem";
  verse-in-version($v, $ver);
  location-mentioned-in-verse($l, $v);
fetch $v;
```

## Project Structure

```
bible-graph/
├── data/
│   ├── books.json
│   ├── versions.json
│   ├── verses.json
│   ├── location_regions.json
│   └── regions.json
├── load_data.py          # Data loading script
├── requirements.txt      # Python dependencies
└── README.md             # README DOCS
```

## License

[GNU General Public License v3.0](LICENSE)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## Support

For questions or issues, please do not hesitate to get in touch with me or open a GitHub issue.

✝️