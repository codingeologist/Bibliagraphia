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

const sitemapUrls = [`${SITE}/`, `${SITE}/read`, `${SITE}/about`];
let count = 0;

for (const version of VERSIONS) {
  const catalogRes = await fetch(`${API}/reader/catalog?version_code=${version}`);
  const catalog = await catalogRes.json();
  const chaptersByBook = catalog.chapters || {};

  for (const book of catalog.books) {
    for (const chapter of chaptersByBook[book.code] || []) {
      const res = await fetch(`${API}/chapter?${new URLSearchParams({
        book_code: book.code, chapter, version_code: version,
      })}`);
      if (!res.ok) {
        console.warn(`skipped ${book.code} ${chapter} ${version}: HTTP ${res.status}`);
        continue;
      }
      const data = await res.json();
      if (!data.verses?.length) {
        console.warn(`skipped ${book.code} ${chapter} ${version}: no verses`);
        continue;
      }

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
    }
  }
}

await writeFile("dist/robots.txt",
`User-agent: *
Allow: /

Sitemap: ${SITE}/sitemap.xml
`);

// Simple XML sitemap (split into pages of 5,000 if exceeded later)
const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemapUrls.map((u) => `  <url><loc>${u.replace(/&/g, "&amp;")}</loc></url>`).join("\n")}
</urlset>`;
await writeFile("dist/sitemap.xml", xml);
console.log(`Prerendered ${count} chapters + robots.txt + sitemap.xml (${sitemapUrls.length} URLs).`);
