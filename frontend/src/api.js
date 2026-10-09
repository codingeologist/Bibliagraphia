const apiBase = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "") ?? "";

async function request(path, options) {
  const response = await fetch(`${apiBase}${path}`, options);
  let body;
  try {
    body = await response.json();
  } catch {
    throw new Error(`API returned an invalid response (${response.status})`);
  }
  if (!response.ok) {
    throw new Error(body.detail || body.error || `Request failed (${response.status})`);
  }
  if (body.error) throw new Error(body.error);
  return body;
}

export const get = (path) => request(path);

export const post = (path, body) =>
  request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

export const apiUrl = (path) => `${apiBase}${path}`;
