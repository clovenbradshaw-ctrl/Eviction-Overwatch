#!/usr/bin/env node
// Batch export: every geocoded property in the eviction feed, cross-referenced
// against Landlord Mapper's public API for ownership. Runs standalone (no
// browser, no LLM in the loop) so a full sweep of tens of thousands of
// properties costs wall-clock time, not tokens.
//
// The Landlord Mapper match for each property is written out as a *revisable
// assertion*, not a verified fact: it's their inference, tagged with what was
// queried, how it matched, and when — never flattened into the property
// record as if it were confirmed ownership. See `landlordMapperAssertion`
// below and the app's own LM_TAG / "we do not recompute or verify their
// matching" language in ../index.html for the same posture in the UI.
//
// Usage:
//   node scripts/export-landlord-mapper-properties.mjs [--concurrency N] [--limit N] [--resume-from FILE] [--out DIR]
//
// Resumable: progress streams to a checkpoint JSONL file (one line per
// completed property) as it goes. Re-running with --resume-from pointing at
// a prior checkpoint skips properties already recorded there, so an
// interrupted run picks back up instead of restarting or re-hitting LM for
// work already done.

import { writeFile, appendFile, readFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..');

const FEED_URL = 'https://n8n.intelechia.com/webhook/bd94bf54-2728-44f9-918b-ade3bad4608b';
const LANDLORDMAPPER_API = 'https://api.landlordmapper.org/nsh';
const LANDLORDMAPPER_SITE = 'https://landlordmapper.org/en/nsh';

function parseArgs(argv) {
  const args = { concurrency: 6, limit: null, resumeFrom: null, out: path.join(REPO_ROOT, 'exports') };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--concurrency') args.concurrency = parseInt(argv[++i], 10);
    else if (a === '--limit') args.limit = parseInt(argv[++i], 10);
    else if (a === '--resume-from') args.resumeFrom = argv[++i];
    else if (a === '--out') args.out = path.resolve(argv[++i]);
  }
  return args;
}

function normalizeAddressKey(addr) {
  if (!addr) return null;
  return String(addr).toUpperCase().replace(/[.,#]/g, '').replace(/\s+/g, ' ').trim();
}

function parseUsDate(v) {
  const m = String(v || '').match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  return m ? new Date(+m[3], +m[1] - 1, +m[2]).getTime() : 0;
}

// Mirrors fetchSnapshot() in index.html: JSONL, one chain INS op per line,
// keyed off docket, `data` kept verbatim.
async function fetchSnapshot() {
  const r = await fetch(FEED_URL);
  if (!r.ok) throw new Error(`HTTP ${r.status} fetching feed`);
  const text = await r.text();
  const cases = {};
  for (const line of text.split('\n')) {
    if (!line) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const docket = obj.docket || (obj.data && obj.data.docket);
    if (!docket || !obj.data) continue;
    cases[docket] = obj.data;
  }
  if (!Object.keys(cases).length) throw new Error('feed returned no cases');
  return cases;
}

// Mirrors buildPropertyIndex() in index.html.
function buildPropertyIndex(cases) {
  const byAddr = new Map();
  for (const [docket, c] of Object.entries(cases)) {
    const lat = Number(c.latitude), lng = Number(c.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    const key = normalizeAddressKey(c.address_formatted) || `${lat.toFixed(5)},${lng.toFixed(5)}`;
    if (!byAddr.has(key)) {
      byAddr.set(key, {
        key, lat, lng,
        addressDisplay: c.address_formatted || c.geocoded_address || key,
        cases: [], landlords: new Map(),
      });
    }
    const p = byAddr.get(key);
    p.cases.push({ docket, file_date: c.file_date, case_type: c.case_type, status: c.status, Plaintiff_1: c.Plaintiff_1 });
    const landlordName = (c.Plaintiff_1 || '').trim();
    if (landlordName) p.landlords.set(landlordName, (p.landlords.get(landlordName) || 0) + 1);
  }
  const properties = [];
  for (const p of byAddr.values()) {
    p.cases.sort((a, b) => (parseUsDate(b.file_date) || 0) - (parseUsDate(a.file_date) || 0));
    p.evictionCount = p.cases.length;
    p.primaryLandlord = [...p.landlords.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || null;
    p.landlordsBreakdown = [...p.landlords.entries()].sort((a, b) => b[1] - a[1]).map(([name, count]) => ({ name, count }));
    properties.push(p);
  }
  return properties;
}

async function lmFetchJson(pathPart, { retries = 3 } = {}) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const r = await fetch(LANDLORDMAPPER_API + pathPart);
    if (r.ok) return r.json();
    if (r.status === 429 || r.status >= 500) {
      const retryAfter = Number(r.headers.get('retry-after')) || Math.min(2 ** attempt, 20);
      await sleep(retryAfter * 1000);
      continue;
    }
    throw new Error(`Landlord Mapper HTTP ${r.status} for ${pathPart}`);
  }
  throw new Error(`Landlord Mapper: exhausted retries for ${pathPart}`);
}

async function lmSearchOne(searchType, searchString) {
  if (!searchString) return null;
  const hits = await lmFetchJson(`/search/json/?search_type=${searchType}&search_string=${encodeURIComponent(searchString)}`).catch(() => []);
  return Array.isArray(hits) && hits.length ? hits[0] : null;
}

function sleep(ms) { return new Promise((res) => setTimeout(res, ms)); }

// Portfolio fetches are cached by landlord_ct_id:landlord_oid across
// properties in this same run — many properties resolve to the same owner,
// and there's no reason to re-fetch their portfolio each time.
const portfolioCache = new Map();

async function fetchPortfolio(landlord_ct_id, landlord_oid) {
  const cacheKey = `${landlord_ct_id}:${landlord_oid}`;
  if (portfolioCache.has(cacheKey)) return portfolioCache.get(cacheKey);
  const p = await lmFetchJson(`/map/portfolio_tab/json/?landlord_ct_id=${landlord_ct_id}&landlord_oid=${landlord_oid}`).catch(() => null);
  portfolioCache.set(cacheKey, p);
  return p;
}

// The core per-property lookup. Mirrors lookupLandlordMapper() in index.html
// (address match first, name-fallback second) but the result is always
// wrapped as a revisable assertion, never merged onto the property as fact.
async function lookupLandlordMapperAssertion(property) {
  const queriedAt = new Date().toISOString();
  try {
    let hit = await lmSearchOne('property', property.addressDisplay);
    let matchedOn = 'address';
    if (!hit && property.primaryLandlord) {
      hit = await lmSearchOne('landlord_name', property.primaryLandlord);
      matchedOn = 'name_fallback';
    }
    if (!hit) {
      return {
        status: 'no_match', matchedOn: null, source: LANDLORDMAPPER_API, asOf: queriedAt, revisable: true,
        caveat: 'No confident match found on Landlord Mapper for this address or plaintiff name at the time of this export. Absence of a match is not evidence of unknown ownership — re-check later or search landlordmapper.org directly.',
        data: null,
      };
    }
    const portfolio = await fetchPortfolio(hit.landlord_ct_id, hit.landlord_oid);
    const allOther = (portfolio?.bldgs_list || []).filter(
      (b) => normalizeAddressKey(b.short_address) !== normalizeAddressKey(property.addressDisplay)
    );
    return {
      status: 'matched', matchedOn, source: LANDLORDMAPPER_API, asOf: queriedAt, revisable: true,
      caveat: 'Ownership inferred by landlordmapper.org from public parcel, business-filing, and taxpayer records — an independent third-party claim, not verified or recomputed by this export. Treat as provisional and re-check landlordmapper.org for the current state.',
      data: {
        buildingUrl: `${LANDLORDMAPPER_SITE}/search/${hit.property_oid}/${hit.landlord_ct_id}/${hit.landlord_oid}/building`,
        portfolioUrl: `${LANDLORDMAPPER_SITE}/search/${hit.property_oid}/${hit.landlord_ct_id}/${hit.landlord_oid}/portfolio`,
        networkName: portfolio?.name || null,
        bldgCount: portfolio?.bldg_count ?? null,
        unitCount: portfolio?.unit_count ?? null,
        otherPropertiesCount: allOther.length,
        otherPropertiesSample: allOther.slice(0, 6).map((b) => b.short_address || b.taxpayer_name || null),
      },
    };
  } catch (e) {
    return {
      status: 'lookup_failed', matchedOn: null, source: LANDLORDMAPPER_API, asOf: queriedAt, revisable: true,
      caveat: 'The Landlord Mapper lookup errored for this property; this is a gap in this export, not a claim that no ownership data exists.',
      error: e.message, data: null,
    };
  }
}

// Small fixed-size concurrency pool — no dependency needed for this.
async function runPool(items, worker, concurrency, onProgress) {
  let idx = 0, done = 0;
  async function next() {
    while (idx < items.length) {
      const i = idx++;
      await worker(items[i], i);
      done++;
      if (onProgress) onProgress(done, items.length);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, next));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  await mkdir(args.out, { recursive: true });

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const checkpointFile = args.resumeFrom || path.join(args.out, `.checkpoint-${stamp}.jsonl`);
  const finalFile = path.join(args.out, `property-landlord-export-${stamp}.json`);

  console.log(`[export] fetching case feed…`);
  const cases = await fetchSnapshot();
  console.log(`[export] ${Object.keys(cases).length} cases fetched; building property index…`);
  let properties = buildPropertyIndex(cases);
  console.log(`[export] ${properties.length} unique geocoded properties.`);

  if (args.limit) properties = properties.slice(0, args.limit);

  // Resume support: skip properties whose key already has a completed line
  // in an existing checkpoint file.
  const already = new Map();
  if (args.resumeFrom && existsSync(args.resumeFrom)) {
    const text = await readFile(args.resumeFrom, 'utf8');
    for (const line of text.split('\n')) {
      if (!line) continue;
      try {
        const rec = JSON.parse(line);
        already.set(rec.key, rec);
      } catch { /* skip malformed checkpoint line */ }
    }
    console.log(`[export] resuming from ${args.resumeFrom}: ${already.size} properties already done.`);
  }

  const todo = properties.filter((p) => !already.has(p.key));
  console.log(`[export] ${todo.length} properties need a Landlord Mapper lookup (concurrency=${args.concurrency}).`);

  const startTs = Date.now();
  await runPool(todo, async (property) => {
    const assertion = await lookupLandlordMapperAssertion(property);
    const record = {
      key: property.key,
      addressDisplay: property.addressDisplay,
      lat: property.lat,
      lng: property.lng,
      evictionCount: property.evictionCount,
      primaryLandlord: property.primaryLandlord,
      landlordsBreakdown: property.landlordsBreakdown,
      firstFileDate: property.cases[property.cases.length - 1]?.file_date || null,
      lastFileDate: property.cases[0]?.file_date || null,
      cases: property.cases,
      landlordMapperAssertion: assertion,
    };
    await appendFile(checkpointFile, JSON.stringify(record) + '\n');
    already.set(property.key, record);
  }, args.concurrency, (done, total) => {
    if (done % 100 === 0 || done === total) {
      const elapsedS = (Date.now() - startTs) / 1000;
      const rate = done / Math.max(elapsedS, 1);
      const etaS = rate > 0 ? Math.round((total - done) / rate) : 0;
      console.log(`[export] ${done}/${total} · ${rate.toFixed(1)}/s · ETA ${Math.round(etaS / 60)}m`);
    }
  });

  // Assemble final export in the property index's own order, from whatever's
  // in `already` (freshly done this run, plus anything resumed from checkpoint).
  const finalRecords = properties.map((p) => already.get(p.key)).filter(Boolean);
  const matched = finalRecords.filter((r) => r.landlordMapperAssertion.status === 'matched').length;
  const noMatch = finalRecords.filter((r) => r.landlordMapperAssertion.status === 'no_match').length;
  const failed = finalRecords.filter((r) => r.landlordMapperAssertion.status === 'lookup_failed').length;

  const output = {
    exportedAt: new Date().toISOString(),
    source: {
      caseFeed: FEED_URL,
      landlordMapper: LANDLORDMAPPER_API,
      note: 'landlordMapperAssertion on each property is landlordmapper.org\'s own inferred ownership match — a revisable third-party claim, not verified fact. See each assertion\'s `caveat` field.',
    },
    propertyCount: finalRecords.length,
    landlordMapperSummary: { matched, no_match: noMatch, lookup_failed: failed },
    properties: finalRecords,
  };

  await writeFile(finalFile, JSON.stringify(output, null, 2));
  console.log(`[export] done. ${finalRecords.length} properties written to ${finalFile}`);
  console.log(`[export] Landlord Mapper: ${matched} matched, ${noMatch} no match, ${failed} failed. Checkpoint kept at ${checkpointFile}`);
}

main().catch((e) => {
  console.error('[export] fatal:', e);
  process.exit(1);
});
