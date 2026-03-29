import type { Context, Config } from "@netlify/edge-functions";

const UPSTREAM = "https://data.norfolk.gov";
const APP_TOKEN = "zMCBjEV4SBnmQJtMvEiRipdFq";

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  // Strip /api/norfolk prefix, forward the rest
  const upstreamPath = url.pathname.replace(/^\/api\/norfolk/, "") + url.search;
  const upstreamUrl = `${UPSTREAM}${upstreamPath}`;

  const upstreamRes = await fetch(upstreamUrl, {
    headers: {
      "X-App-Token": APP_TOKEN,
      "Accept": "application/json",
    },
  });

  const body = await upstreamRes.text();

  return new Response(body, {
    status: upstreamRes.status,
    headers: {
      "Content-Type": upstreamRes.headers.get("Content-Type") || "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });
};

export const config: Config = {
  path: "/api/norfolk/*",
};
