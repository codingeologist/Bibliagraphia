"""STEPBible data used by build_db.py to link figures to verses.

Two CC BY 4.0 datasets from STEP Bible (https://www.STEPBible.org, based on
work at Tyndale House Cambridge) are used:

* TIPNR  - every person in the Bible with a unique id and all their verse refs
* TVTMS  - rules for converting verse numbers between the English, Hebrew,
           Latin and Greek numbering traditions

STEPBible asks that their data is not redistributed, so it is not committed
to this repo. It is downloaded from their GitHub on first build and cached
in data/.stepbible/ (gitignored). Set BIBLE_STEP_DIR to use another cache.

TIPNR refs use standard English numbering (with v.0 for a Psalm title). Our
Bibles do not all follow it - the Vulgate and Douay-Rheims use Latin
numbering (Psalm 23 is their 22, Daniel 3 has 100 verses, ...), and our
KJV/LEB fold Psalm titles into v.1. TVTMS tells us, per passage, which
tradition a Bible follows (via tests such as "Psa.3:9=Last") and how the
refs line up, so each standard ref is converted per version before it is
joined to our verses.
"""
from __future__ import annotations

import re
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

_BASE = "https://raw.githubusercontent.com/STEPBible/STEPBible-Data/master/"
FILES = {
    "tipnr": "Proper Nouns/TIPNR - Translators Individualised Proper Names "
             "with all References - STEPBible.org CC BY.txt",
    "tvtms": "Versification/TVTMS - Translators Versification Traditions with "
             "Methodology for Standardisation for Eng+Heb+Lat+Grk+Others - "
             "STEPBible.org CC BY.txt",
}

# STEPBible book abbreviations -> our book codes (data/books.json).
BOOKS = {
    "Gen": "GEN", "Exo": "EXO", "Lev": "LEV", "Num": "NUM", "Deu": "DEU",
    "Jos": "JOS", "Jdg": "JDG", "Rut": "RUT", "1Sa": "1SA", "2Sa": "2SA",
    "1Ki": "1KI", "2Ki": "2KI", "1Ch": "1CH", "2Ch": "2CH", "Ezr": "EZR",
    "Neh": "NEH", "Est": "EST", "Job": "JOB", "Psa": "PSA", "Pro": "PRO",
    "Ecc": "ECC", "Sng": "SOL", "Isa": "ISA", "Jer": "JER", "Lam": "LAM",
    "Ezk": "EZE", "Dan": "DAN", "Hos": "HOS", "Jol": "JOE", "Amo": "AMO",
    "Oba": "OBA", "Jon": "JON", "Mic": "MIC", "Nam": "NAH", "Hab": "HAB",
    "Zep": "ZEP", "Hag": "HAG", "Zec": "ZEC", "Mal": "MAL", "Tob": "TOB",
    "Jdt": "JDT", "Wis": "WIS", "Sir": "SIR", "Bar": "BAR", "1Ma": "1MA",
    "2Ma": "2MA", "Mat": "MAT", "Mrk": "MAR", "Luk": "LUK", "Jhn": "JOH",
    "Act": "ACT", "Rom": "ROM", "1Co": "1CO", "2Co": "2CO", "Gal": "GAL",
    "Eph": "EPH", "Php": "PHI", "Col": "COL", "1Th": "1TH", "2Th": "2TH",
    "1Ti": "1TI", "2Ti": "2TI", "Tit": "TIT", "Phm": "PHM", "Heb": "HEB",
    "Jas": "JAM", "1Pe": "1PE", "2Pe": "2PE", "1Jn": "1JO", "2Jn": "2JO",
    "3Jn": "3JO", "Jud": "JUD", "Rev": "REV",
}

_BOOKS_CI = {k.lower(): v for k, v in BOOKS.items()}


def _book(abbr: str | None) -> str | None:
    """Our book code for a STEPBible abbreviation (TVTMS mixes "Psa"/"PSA")."""
    return _BOOKS_CI.get((abbr or "").lower())


TITLE = 0  # verse number used for a Psalm title ("Psa.3:Title" / TIPNR "Psa.3.0")


def fetch(cache: Path) -> dict[str, Path] | None:
    """Return local paths of the STEPBible files, downloading any missing.

    Returns None (and prints why) if a file is missing and cannot be
    downloaded, so the build can carry on without figure edges.
    """
    cache.mkdir(parents=True, exist_ok=True)
    paths = {}
    for key, name in FILES.items():
        path = cache / f"{key}.txt"
        if not path.exists():
            url = _BASE + urllib.parse.quote(name)
            try:
                with urllib.request.urlopen(url, timeout=60) as resp:
                    path.write_bytes(resp.read())
            except OSError as exc:
                print(f"  ! could not download STEPBible {key.upper()} ({exc}); "
                      "figures will have no verse edges")
                return None
        paths[key] = path
    return paths


# --------------------------------------------------------------------------- #
# TIPNR
# --------------------------------------------------------------------------- #
_SKIP_FORMS = ("– Total", "– Group", "– (same form with alt. meaning)")
_TIPNR_REF = re.compile(r"\s*([1-4]?[A-Za-z]{2,3})\.(\d+)\.(\d+)")


def tipnr_refs(path: Path) -> dict[str, list[tuple[str, int, int]]]:
    """TIPNR person id (e.g. "Peter@Mat.4.18-2Pe") -> standard refs.

    Each person record has one line per name form ("– Named", "– Spelled",
    ...) whose last column lists every verse using that form; the union
    of those is every verse the person appears in. "Group" forms (the
    nation or tribe named after a person, e.g. Israelites, Jews) and
    "alt. meaning" forms (where only some translations read the name)
    are skipped. Refs are returned as (our book code, chapter, verse) in
    standard English numbering.
    """
    people: dict[str, list[tuple[str, int, int]]] = {}
    refs = None
    in_people = False
    for raw in path.open(encoding="utf-8-sig"):
        line = raw.rstrip("\r\n")
        if line.startswith("$=========="):
            in_people = "PERSON" in line and "PLACE" not in line
            refs = None
            continue
        if not in_people:
            continue
        cols = line.split("\t")
        if refs is None:
            if "@" in cols[0] and "=" in cols[0] and not line.startswith(("–", "@")):
                refs = people.setdefault(cols[0].split("=")[0].strip(), [])
            continue
        if line.startswith("– ") and not line.startswith(_SKIP_FORMS):
            ref_col = next((c for c in reversed(cols) if _TIPNR_REF.match(c)), "")
            for part in ref_col.split(";"):
                m = _TIPNR_REF.match(part)
                if m and _book(m[1]):
                    ref = (_book(m[1]), int(m[2]), int(m[3]))
                    if ref not in refs:
                        refs.append(ref)
    return people


# --------------------------------------------------------------------------- #
# TVTMS
# --------------------------------------------------------------------------- #
def _expand(cell: str) -> list[tuple[str, int, int]]:
    """Refs in a TVTMS alignment cell, e.g. "Psa.3:2-8", "Gen.5:31.1",
    "Absent [=Gen.3:1]", "Psa.9:22-39", "Gen.5:32; 6:1". Subverses
    (".1") fold into their verse; cells we can't read give []."""
    cell = cell.strip()
    if cell.startswith("Absent"):
        m = re.search(r"\[=(.*?)\]", cell)
        cell = m[1] if m else ""
    cell = cell.split("[")[0].strip()
    out: list[tuple[str, int, int]] = []
    book = chapter = None
    for part in re.split(r"[;,]", cell):
        part = part.strip()
        m = re.match(r"(?:([1-4]?[A-Za-z]{2,3})\.)?(?:(\d+):)?(\d+|Title)(?:\.\d+)?"
                     r"(?:-(\d+)(?:\.\d+)?)?$", part)
        if not m:
            continue
        book = _book(m[1]) or book
        chapter = int(m[2]) if m[2] else chapter
        if book is None or chapter is None:
            continue
        start = TITLE if m[3] == "Title" else int(m[3])
        end = int(m[4]) if m[4] else start
        out += [(book, chapter, v) for v in range(start, end + 1)]
    return list(dict.fromkeys(out))


class _Bible:
    """Verse lookup used to evaluate TVTMS tests against one of our versions."""

    def __init__(self, verses: dict[tuple[str, int, int], int]):
        self.words = verses  # (book, chapter, verse) -> word count
        self.last: dict[tuple[str, int], int] = defaultdict(int)
        for (b, c, v) in verses:
            self.last[(b, c)] = max(self.last[(b, c)], v)

    def _count(self, expr: str) -> int | None:
        total = 0
        for term in expr.split("+"):
            term, mult = (term.split("*") + ["1"])[:2]
            m = re.match(r"([1-4]?[A-Za-z]{2,3})\.(\d+):(\d+)(?:\.(\d+))?$", term.strip())
            if not m or not _book(m[1]):
                return None
            sub = int(m[4] or 0)
            words = self.words.get((_book(m[1]), int(m[2]), int(m[3])), 0) if sub == 0 else 0
            total += words * int(mult)
        return total

    def test(self, test: str) -> bool | None:
        """True/False for one TVTMS test, None if we can't evaluate it."""
        test = test.strip()
        m = re.match(r"([1-4]?[A-Za-z]{2,3})\.(\d+):(\d+|TextBeforeV1)(?:\.(\d+))?"
                     r"=(Last|Exist|NotExist)$", test, re.I)
        if m:
            if not _book(m[1]):
                return None
            key = (_book(m[1]), int(m[2]))
            kind = m[5].lower()
            if m[3] == "TextBeforeV1":  # our Bibles number every title as a verse
                return kind == "notexist"
            verse, sub = int(m[3]), int(m[4] or 0)
            exists = sub == 0 and self.words.get((*key, verse), 0) > 0
            if kind == "last":
                return sub == 0 and self.last.get(key) == verse
            return exists if kind == "exist" else not exists
        m = re.match(r"(.+?)([<>])(.+)$", test)
        if m:
            a, b = self._count(m[1]), self._count(m[3])
            if a is None or b is None:
                return None
            return a < b if m[2] == "<" else a > b
        return None

    def passes(self, tests: list[str]) -> bool:
        return all(self.test(t) is True for t in tests)


def _sections(path: Path):
    """Yield (column names, tests per column, alignment rows) for each
    section of the TVTMS condensed table.

    Sections come in two layouts: column names on the "$" header line
    with "TEST:" rows giving each column's tests, or one line per group
    of traditions ("Hebrew + Greek<TAB> & tests") followed by a
    "BIBLES" line naming the columns.
    """
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    start = next(i for i, l in enumerate(lines) if l.startswith("#DataStart(Condensed)"))
    end = next(i for i, l in enumerate(lines) if l.startswith("#DataEnd(Condensed)"))
    block: list[list[str]] = []
    for line in lines[start + 1:end] + ["$END"]:
        if line.startswith("$"):
            if block:
                yield _section(block)
            block = []
        block.append([c.strip() for c in line.split("\t")])


def _section(block: list[list[str]]):
    header = block[0]
    names = [c for c in header[1:] if c]
    tests: dict[str, list[str]] = defaultdict(list)
    group_tests: list[tuple[list[str], list[str]]] = []
    rows = []
    for cols in block[1:]:
        first = cols[0]
        if first.startswith("TEST"):
            for name, cell in zip(names, cols[1:]):
                tests[name] += [t for t in re.split(r"\s*&\s*", cell) if t]
        elif first == "BIBLES":
            names = [c for c in cols[1:] if c]
        elif len(cols) > 1 and cols[1].startswith("&"):
            group = [g.strip() for g in first.split("+")]
            group_tests.append((group, [t for t in re.split(r"\s*&\s*", cols[1]) if t]))
        elif len(cols) > 2 and first and not first.startswith((",", "'", "#")):
            rows.append(cols[1:1 + len(names)])
    for group, ts in group_tests:
        for name in names:
            if any(name == g or name.startswith(g + " ") or g.startswith(name) for g in group):
                tests[name] += ts
    return names, dict(tests), rows


def versification(path: Path, verses: dict[str, dict[tuple[str, int, int], int]]):
    """Per version, a map standard ref -> that version's refs, for every
    standard ref whose number differs in that version.

    `verses` is {version_code: {(book, chapter, verse): word count}}.
    For each TVTMS section we pick the column whose tests pass for that
    version and line its refs up with the English column. Refs not in
    the map keep their standard number (except Psalm titles, v.0, which
    fold into v.1 when a version has no separate title verse).
    """
    sections = list(_sections(path))
    maps: dict[str, dict[tuple, list[tuple]]] = {}
    for code, vs in verses.items():
        bible = _Bible(vs)
        mapping: dict[tuple, list[tuple]] = {}
        for names, tests, rows in sections:
            if not names or not names[0].startswith("English"):
                continue
            col = next((i for i, n in enumerate(names) if tests.get(n) and bible.passes(tests[n])), None)
            if col is None or col == 0:
                continue
            for row in rows:
                if len(row) <= col:
                    continue
                eng, tgt = _expand(row[0]), _expand(row[col])
                if not eng or not tgt:
                    continue
                if len(eng) == len(tgt):
                    pairs = [(e, [t]) for e, t in zip(eng, tgt)]
                elif len(eng) == 1:
                    pairs = [(eng[0], tgt)]
                elif len(tgt) == 1:
                    pairs = [(e, tgt) for e in eng]
                else:
                    pairs = [(e, [t]) for e, t in zip(eng, tgt)]
                for e, ts in pairs:
                    if ts != [e]:
                        mapping[e] = ts
        maps[code] = mapping
    return maps


def convert(ref: tuple[str, int, int], mapping: dict, verses: dict) -> list[tuple[str, int, int]]:
    """Refs in one version for a standard ref (see versification())."""
    if ref in mapping:
        return mapping[ref]
    if ref[2] == TITLE and ref not in verses:
        return [(ref[0], ref[1], 1)]
    return [ref]
