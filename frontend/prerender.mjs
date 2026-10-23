// frontend/prerender.mjs
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

const API = process.env.PRERENDER_API_BASE || "https://api.bibliagraphia.com";
const SITE = "https://bibliagraphia.com";
const VERSIONS = (process.env.PRERENDER_VERSIONS || "KJV")
  .split(",")
  .map((v) => v.trim().toUpperCase())
  .filter(Boolean);

const escape = (s) => String(s ?? "")
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;").replaceAll('"', "&quot;");

// Fetch JSON with retries and a timeout; returns null on final failure
// instead of throwing, so one bad chapter can never crash the whole build.
async function fetchJson(url, { retries = 5, timeoutMs = 30000 } = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      if (attempt === retries) {
        console.warn(`giving up: ${url}: ${err.message}`);
        return null;
      }
      await new Promise((r) => setTimeout(r, 500 * attempt)); // 0.5s, 1s backoff
    } finally {
      clearTimeout(timer);
    }
  }
  return null;
}

const sitemapUrls = [`${SITE}/`, `${SITE}/read`, `${SITE}/about`];
let count = 0;
let skipped = 0;

for (const version of VERSIONS) {
  const catalog = await fetchJson(`${API}/reader/catalog?version_code=${version}`);
  if (!catalog?.books) {
    console.warn(`no catalog for ${version}, skipping version`);
    continue;
  }
  const chaptersByBook = catalog.chapters || {};

  for (const book of catalog.books) {
    for (const chapter of chaptersByBook[book.code] || []) {
      const data = await fetchJson(`${API}/chapter?${new URLSearchParams({
        book_code: book.code, chapter, version_code: version,
      })}`);
      if (!data?.verses?.length) {
        console.warn(`skipped ${book.code} ${chapter} ${version}: no data`);
        skipped++;
        continue;
      }

      try {
        const canonical = `${SITE}/read?book=${book.code}&chapter=${chapter}&version=${version}`;
        const versesHtml = data.verses.map((v) =>
          `<p class="ssr-verse"><sup>${v.number}</sup> ${escape(v.text)}</p>`).join("\n");
        const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${escape(data.book_name)} ${chapter} (${version}) — Biblia Graphia</title>
  <meta name="description" content="Read ${escape(data.book_name)} ${chapter} in the ${version}. Places, people and cross-references, mapped in a graph." />
  <link rel="canonical" href="${canonical}" />
  <link rel="icon" href="/favicon.svg?v=3" type="image/svg+xml" />
</head>
<body>
  <div id="ssr-content">
    <h1>${escape(data.book_name)} ${chapter} — ${version}</h1>
    ${versesHtml}
    <p><a href="${canonical}">Open the full interactive reader</a></p>
  </div>
  <div id="root"></div>
  <script type="module" src="/assets/main.js"></script>
</body>
</html>`;
        const file = path.join("dist", "prerender",
          `${book.code}-${chapter}-${version}.html`);
        await mkdir(path.dirname(file), { recursive: true });
        await writeFile(file, html);
        sitemapUrls.push(canonical);
        count++;
        if (count % 200 === 0) console.log(`prerendered ${count}...`);
      } catch (err) {
        console.warn(`skipped ${book.code} ${chapter} ${version}: ${err.message}`);
        skipped++;
      }
    }
  }
}

await writeFile("dist/robots.txt",
`User-agent: *
Allow: /

Sitemap: ${SITE}/sitemap.xml
`);

const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemapUrls.map((u) => `  <url><loc>${u.replace(/&/g, "&amp;")}</loc></url>`).join("\n")}
</urlset>`;
await writeFile("dist/sitemap.xml", xml);
console.log(`Prerendered ${count} chapters (${skipped} skipped) + robots.txt + sitemap.xml (${sitemapUrls.length} URLs).`);