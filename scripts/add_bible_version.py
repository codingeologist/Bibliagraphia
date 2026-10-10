#!/usr/bin/env python3
"""Add a Bible version to the project's JSON files.

Input: a Bible JSON shaped like LEB.json:
    {"books": [{"name": "Genesis", "chapters": [{"chapter": 1, "verses": [{"verse": 1, "text": "..."}]}]}]}

Updates (all in one go, nothing is written if anything fails):
    versions.json         adds {"code", "name", "full_name"}
    verses.json           adds one row per verse (replacing any earlier rows with the same code)
    location_regions.json adds "<name>" (book name) and "<name>_text" to every row
    books.json            adds "<name>" (book name, null if the version lacks the book)

Books are matched to codes by NAME, so partial files (e.g. New Testament only) work. Books with
no text at all (placeholders) are skipped. Book names are copied from the KJV rows in
verses.json so every version uses the same names. Verses a version doesn't have get null in
location_regions.json; blank verses in the source stay as blank rows in verses.json.

Examples:
    python add_bible_version.py LEB.json --full-name "Lexham English Bible"
    python add_bible_version.py StatResGNT.json --code SRG --name statresgnt \\
        --full-name "Statistical Restoration Greek New Testament" --strip "˚¶"
    python add_bible_version.py LEB.json --dry-run          # report only, write nothing

--code defaults to the file name upper-cased (LEB.json -> LEB) and --name to its lower-case form.
If the code already exists in versions.json, its name and full name are reused, so re-running
only needs the file. Re-running is safe: the version's old data is replaced, not duplicated.
"""
import argparse
import json
import re
import sys
from pathlib import Path

# (code, canonical name) - the 66 books of the KJV canon
BOOKS = [
    ("GEN", "Genesis"), ("EXO", "Exodus"), ("LEV", "Leviticus"), ("NUM", "Numbers"),
    ("DEU", "Deuteronomy"), ("JOS", "Joshua"), ("JDG", "Judges"), ("RUT", "Ruth"),
    ("1SA", "1 Samuel"), ("2SA", "2 Samuel"), ("1KI", "1 Kings"), ("2KI", "2 Kings"),
    ("1CH", "1 Chronicles"), ("2CH", "2 Chronicles"), ("EZR", "Ezra"), ("NEH", "Nehemiah"),
    ("EST", "Esther"), ("JOB", "Job"), ("PSA", "Psalms"), ("PRO", "Proverbs"),
    ("ECC", "Ecclesiastes"), ("SOL", "Song of Solomon"), ("ISA", "Isaiah"), ("JER", "Jeremiah"),
    ("LAM", "Lamentations"), ("EZE", "Ezekiel"), ("DAN", "Daniel"), ("HOS", "Hosea"),
    ("JOE", "Joel"), ("AMO", "Amos"), ("OBA", "Obadiah"), ("JON", "Jonah"), ("MIC", "Micah"),
    ("NAH", "Nahum"), ("HAB", "Habakkuk"), ("ZEP", "Zephaniah"), ("HAG", "Haggai"),
    ("ZEC", "Zechariah"), ("MAL", "Malachi"),
    ("MAT", "Matthew"), ("MAR", "Mark"), ("LUK", "Luke"), ("JOH", "John"), ("ACT", "Acts"),
    ("ROM", "Romans"), ("1CO", "1 Corinthians"), ("2CO", "2 Corinthians"), ("GAL", "Galatians"),
    ("EPH", "Ephesians"), ("PHI", "Philippians"), ("COL", "Colossians"),
    ("1TH", "1 Thessalonians"), ("2TH", "2 Thessalonians"), ("1TI", "1 Timothy"),
    ("2TI", "2 Timothy"), ("TIT", "Titus"), ("PHM", "Philemon"), ("HEB", "Hebrews"),
    ("JAM", "James"), ("1PE", "1 Peter"), ("2PE", "2 Peter"), ("1JO", "1 John"),
    ("2JO", "2 John"), ("3JO", "3 John"), ("JUD", "Jude"), ("REV", "Revelation"),
]
CODE_BY_NAME = {name.lower(): code for code, name in BOOKS}
CODE_BY_NAME.update({  # a few common alternative spellings
    "song of songs": "SOL", "canticles": "SOL", "psalm": "PSA",
    "revelation of john": "REV", "revelation of st. john": "REV", "the revelation": "REV",
})
ROMAN = {"I": "1", "II": "2", "III": "3"}


def normalise_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"^(I{1,3})\.? ", lambda m: ROMAN[m.group(1)] + " ", name)
    return re.sub(r"^(\d)(st|nd|rd)? ", r"\1 ", name).lower()


def book_code(name: str):
    return CODE_BY_NAME.get(normalise_name(name))


# ---------- serialisation (reproduces the existing files byte-for-byte) ----------

def dump_flat_rows(rows: list, indent_keys: str = "        ") -> str:
    """verses.json / location_regions.json style: no space after colons, \\uXXXX and \\/ escapes."""
    objs = []
    for r in rows:
        lines = [f'{indent_keys}"{k}":{json.dumps(v).replace("/", chr(92) + "/")}' for k, v in r.items()]
        objs.append("    {\n" + ",\n".join(lines) + "\n    }")
    return "[\n" + ",\n".join(objs) + "\n]"


def dump_pretty(obj) -> str:
    """versions.json / books.json style: standard indent=4, real UTF-8."""
    return json.dumps(obj, indent=4, ensure_ascii=False)


def keep_trailing_newline(new: str, original_raw: str) -> str:
    return new + "\n" if original_raw.endswith("\n") else new


# ---------- helpers ----------

def insert_after_last(row: dict, new_key: str, new_val, candidates) -> dict:
    """Copy of row with new_key set. If it already exists it is updated in place; otherwise it is
    placed right after the last present candidate key."""
    if new_key in row:
        return {k: (new_val if k == new_key else v) for k, v in row.items()}
    present = [k for k in row if k in candidates]
    anchor = present[-1] if present else None
    out = {}
    for k, v in row.items():
        out[k] = v
        if k == anchor:
            out[new_key] = new_val
    if anchor is None:
        out[new_key] = new_val
    return out


def clean_text(text: str, strip: str) -> str:
    if not strip:
        return text
    text = text.translate({ord(ch): None for ch in strip})
    return re.sub(r"\s{2,}", " ", text).strip()


def read_json(path: Path):
    """Return (parsed, raw) or (None, '') if the file doesn't exist."""
    if not path.exists():
        return None, ""
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw), raw


def build_rows(src: Path, code: str, full_name: str, kjv_names: dict, strip: str):
    books = json.loads(src.read_text(encoding="utf-8"))["books"]
    rows, skipped_empty, skipped_unknown = [], [], []
    for b in books:
        has_text = any(v["text"].strip() for c in b["chapters"] for v in c["verses"])
        bc = book_code(b["name"])
        if bc is None or not has_text:
            (skipped_unknown if (bc is None and has_text) else skipped_empty).append(b["name"])
            continue
        name = kjv_names.get(bc) or BOOKS_BY_CODE[bc]
        for c in b["chapters"]:
            for v in c["verses"]:
                rows.append({
                    "version_code": code, "version": full_name, "book_code": bc, "book": name,
                    "chapter": c["chapter"], "verse": v["verse"], "text": clean_text(v["text"], strip),
                })
    return rows, skipped_empty, skipped_unknown


BOOKS_BY_CODE = {code: name for code, name in BOOKS}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("src", type=Path, help="Bible JSON in the LEB.json format")
    ap.add_argument("--code", help="version code, e.g. LEB (default: file name upper-cased)")
    ap.add_argument("--name", help="short name used for field names, e.g. leb (default: lower-cased code)")
    ap.add_argument("--full-name", help='full name, e.g. "Lexham English Bible"')
    ap.add_argument("--strip", default="", help="characters to remove from the text, e.g. '˚¶'")
    ap.add_argument("--dir", default=".", type=Path, help="folder holding the JSON files (default: current)")
    ap.add_argument("--dry-run", action="store_true", help="report what would change; write nothing")
    a = ap.parse_args()

    d = a.dir
    versions_p, verses_p = d / "versions.json", d / "verses.json"
    loc_p, books_p = d / "location_regions.json", d / "books.json"

    versions, versions_raw = read_json(versions_p)
    versions = versions or []
    code = (a.code or a.src.stem).upper()
    existing = next((v for v in versions if v["code"] == code), None)
    name = a.name or (existing["name"] if existing else code.lower())
    full_name = a.full_name or (existing["full_name"] if existing else None)
    if not full_name:
        sys.exit("--full-name is required for a new version")
    if not re.fullmatch(r"[a-z][a-z0-9]*", name):
        sys.exit(f'--name must be lower-case letters/digits (got "{name}")')
    text_key = f"{name}_text"

    # ---- verses.json ----
    verses, verses_raw = read_json(verses_p)
    verses = verses or []
    kjv_names = {}
    for v in verses:
        if v["version_code"] == "KJV":
            kjv_names.setdefault(v["book_code"], v["book"])
    new_rows, skipped_empty, skipped_unknown = build_rows(a.src, code, full_name, kjv_names, a.strip)
    if not new_rows:
        sys.exit(f"No usable verses found in {a.src}")
    if skipped_unknown:
        print(f"WARNING: skipped books with text but unrecognised names: {skipped_unknown}")
    if skipped_empty:
        print(f"Skipped {len(skipped_empty)} books with no text (placeholders)")
    blank = [r for r in new_rows if not r["text"].strip()]
    kept = [v for v in verses if v["version_code"] != code]
    out_verses, placed = [], False
    for v in verses:                      # an existing version is replaced where it already sits
        if v["version_code"] == code:
            if not placed:
                out_verses.extend(new_rows)
                placed = True
        else:
            out_verses.append(v)
    if not placed:
        out_verses.extend(new_rows)
    book_name = {}
    for r in new_rows:
        book_name.setdefault(r["book_code"], r["book"])
    text_of = {(r["book_code"], r["chapter"], r["verse"]): r["text"] for r in new_rows}

    # ---- versions.json ----
    entry = {"code": code, "name": name, "full_name": full_name}
    out_versions = [entry if v["code"] == code else v for v in versions]
    if not existing:
        out_versions.append(entry)
    other_names = {v["name"] for v in out_versions if v["name"] != name}

    outputs = {
        versions_p: keep_trailing_newline(dump_pretty(out_versions), versions_raw or "\n"),
        verses_p: keep_trailing_newline(dump_flat_rows(out_verses), verses_raw),
    }
    report = [
        f"{versions_p.name}: {'updated' if existing else 'added'} {code}",
        f"{verses_p.name}: {len(new_rows)} {code} verses"
        + (f" (replacing {len(verses) - len(kept)} old rows)" if len(verses) != len(kept) else "")
        + f"; total {len(out_verses)}"
        + (f"; {len(blank)} blank verses kept as empty rows" if blank else ""),
    ]

    # ---- location_regions.json ----
    loc, loc_raw = read_json(loc_p)
    if loc is not None:
        name_keys, text_keys = other_names, {f"{n}_text" for n in other_names}
        out_loc, have = [], 0
        for r in loc:
            t = text_of.get((r["book_code"], r["chapter"], r["verse"]))
            t = t if t and t.strip() else None
            have += t is not None
            r = insert_after_last(r, name, book_name.get(r["book_code"]), name_keys)
            r = insert_after_last(r, text_key, t, text_keys)
            out_loc.append(r)
        outputs[loc_p] = keep_trailing_newline(dump_flat_rows(out_loc), loc_raw)
        report.append(f"{loc_p.name}: {len(out_loc)} rows; {have} have {code} text, {len(out_loc) - have} null")
    else:
        report.append(f"{loc_p.name}: not found, skipped")

    # ---- books.json ----
    books, books_raw = read_json(books_p)
    if books is not None:
        out_books = [insert_after_last(b, name, book_name.get(b["code"]), other_names) for b in books]
        outputs[books_p] = keep_trailing_newline(dump_pretty(out_books), books_raw)
        missing = [b["code"] for b in out_books if b[name] is None]
        report.append(f"{books_p.name}: {len(out_books)} books; {len(out_books) - len(missing)} named, "
                      f"null for {len(missing)}" + (f" ({', '.join(missing[:8])}{'...' if len(missing) > 8 else ''})" if missing else ""))
    else:
        report.append(f"{books_p.name}: not found, skipped")

    print("\n".join(report))
    if a.dry_run:
        print("Dry run: nothing written.")
        return
    for path, text in outputs.items():          # everything built successfully - now write
        path.write_text(text, encoding="utf-8")
    print("Done.")


if __name__ == "__main__":
    main()
