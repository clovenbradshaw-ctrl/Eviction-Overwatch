#!/usr/bin/env node
/* Build data/cases.json — the static hydration snapshot the app loads on cold
 * start. It folds the entire archive.org EO chain (latest aggregate + every
 * chain block) into a resolved { docket: record } map, then writes it next to
 * the app so the browser can hydrate from one same-origin request instead of
 * walking archive.org and downloading the ~70k-row genesis every cold load.
 *
 * The output mirrors what the browser's loadChainCases() builds:
 *   { cases:{docket:rec}, updated_at, cutoff, seenIds:[…], generated_at }
 * where `cutoff` is the newest archive.org addeddate folded in and `seenIds`
 * are every block already incorporated — so the client folds only newer blocks.
 *
 * Run it anywhere with outbound access to archive.org (a scraper host, a laptop,
 * a CI runner — NOT the sandbox, which firewalls archive.org):
 *   node tools/build-cases-snapshot.mjs
 * Then commit the regenerated data/cases.json.
 *
 * Keep the fold logic below in sync with the applyOp/foldBlock pair in
 * index.html — they must produce identical state.
 */
import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const IA_SEARCH = 'https://archive.org/advancedsearch.php';
const IA_METADATA = 'https://archive.org/metadata';
const IA_DOWNLOAD = 'https://archive.org/download';
const CHAIN_SUBJECT = 'nashville-evictions-chain';
const AGGREGATE_SUBJECT = 'nashville-evictions-aggregate';

const OUT_PATH = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'data', 'cases.json');

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
  return r.json();
}

// All archive.org items tagged with `subject`, newest-first (addeddate desc).
async function findBlocks(subject) {
  const url = new URL(IA_SEARCH);
  url.searchParams.set('q', `subject:"${subject}"`);
  ['identifier', 'date', 'subject', 'title', 'addeddate'].forEach(f => url.searchParams.append('fl[]', f));
  url.searchParams.append('sort[]', 'addeddate desc');
  url.searchParams.set('rows', '500');
  url.searchParams.set('output', 'json');
  const j = await fetchJson(url.toString());
  return (j.response && j.response.docs) || [];
}

// Fold one EO transformation into a { docket: case } dict. Mirrors index.html.
function applyOp(state, t) {
  if (!t || !t.docket) return;
  const d = t.docket;
  switch (t.op) {
    case 'INS':
      state[d] = { ...t.data };
      break;
    case 'DEF': {
      const c = state[d] || (state[d] = {});
      for (const [k, v] of Object.entries(t.changes || {})) {
        c[k] = (v && typeof v === 'object' && 'new' in v) ? v.new : v;
      }
      break;
    }
    case 'SEG':
      (state[d] || (state[d] = {})).status = t.to_status;
      break;
    case 'CON':
      (state[d] || (state[d] = {})).enriched = true;
      break;
    default:
      break;
  }
}

// Resolve an item id to its .jsonl block and fold every op into `state`.
async function foldBlock(identifier, state) {
  const meta = await fetchJson(`${IA_METADATA}/${identifier}`);
  const file = (meta.files || []).find(f => f.name && f.name.endsWith('.jsonl'));
  if (!file) throw new Error(`no .jsonl file in item ${identifier}`);
  const r = await fetch(`${IA_DOWNLOAD}/${identifier}/${file.name}`);
  if (!r.ok) throw new Error(`download ${identifier}: HTTP ${r.status}`);
  const text = await r.text();
  let header = null;
  for (const line of text.split('\n')) {
    const s = line.trim();
    if (!s) continue;
    let obj;
    try { obj = JSON.parse(s); } catch (_) { continue; }
    if (obj._block) { header = obj; continue; }
    applyOp(state, obj);
  }
  return header;
}

async function main() {
  const state = {};
  const seen = new Set();
  let cutoff = null, latestTs = null;

  // Latest aggregate first (resolved-state snapshot), if any exist yet.
  const aggs = await findBlocks(AGGREGATE_SUBJECT).catch(() => []);
  if (aggs.length) {
    const h = await foldBlock(aggs[0].identifier, state);
    seen.add(aggs[0].identifier);
    cutoff = aggs[0].addeddate || null;
    latestTs = (h && h.ts) || aggs[0].addeddate || latestTs;
    console.error(`folded aggregate ${aggs[0].identifier}`);
  }

  // Every chain block newer than that aggregate, oldest-first (ops are ordered).
  const chain = await findBlocks(CHAIN_SUBJECT);
  const fresh = chain
    .filter(b => !cutoff || (b.addeddate || '') > cutoff)
    .sort((a, b) => (a.addeddate || '').localeCompare(b.addeddate || ''));
  for (const b of fresh) {
    const h = await foldBlock(b.identifier, state);
    seen.add(b.identifier);
    if (b.addeddate && (!cutoff || b.addeddate > cutoff)) cutoff = b.addeddate;
    latestTs = (h && h.ts) || b.addeddate || latestTs;
    console.error(`folded block ${b.identifier} (${b.addeddate || '?'})`);
  }

  const docketCount = Object.keys(state).length;
  if (!docketCount) throw new Error('no cases found in the chain — refusing to write an empty snapshot');

  const payload = {
    cases: state,
    updated_at: latestTs,
    cutoff,
    seenIds: [...seen],
    generated_at: new Date().toISOString(),
  };
  await mkdir(dirname(OUT_PATH), { recursive: true });
  await writeFile(OUT_PATH, JSON.stringify(payload));
  console.error(`wrote ${OUT_PATH}: ${docketCount} cases, ${seen.size} blocks, cutoff ${cutoff}`);
}

main().catch(err => { console.error(err); process.exit(1); });
