#!/usr/bin/env node
// Batch export: Metro Codes' "Property Standards Requests" dataset (property
// standards / code violations), pulled directly from Nashville's public
// ArcGIS FeatureServer -- no browser, no auth, no LLM in the loop.
//
// This is Metro's own record of a code-enforcement request against a
// property (source, problem reported, status, resolution) -- a public
// government record, not a third-party inference the way Landlord Mapper's
// ownership matches are. Still keyed by the SAME normalizeAddressKey() as
// every other property join in this app, so the frontend can look violations
// up for a case row the identical way it looks up a Landlord Mapper match.
//
// Usage:
//   node scripts/export-code-violations.mjs [--out DIR]
//
// The dataset is "a rolling three-year period" per its own metadata, not
// full history -- unlike the case data, this snapshot goes stale on Metro's
// own schedule, not just this export's age.

import { writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..');

const SERVICE_URL = 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Property_Standards_Violations_2/FeatureServer/0';
const PAGE_SIZE = 1000; // the service's own maxRecordCount

const OUT_FIELDS = [
  'ObjectId', 'Request_Nbr', 'Date_Received', 'Property_APN', 'Property_Address',
  'City', 'State', 'Property_Owner', 'Complaint_Source', 'Reported_Problem', 'Status',
  'Cncl_Dist', 'Last_Activity_Date', 'Last_Activity', 'Last_Act__Result', 'Violations_Noted',
  'Lat', 'Lon', 'ZIP', 'Subtype_Description',
];

function parseArgs(argv) {
  const args = { out: path.join(REPO_ROOT, 'exports') };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--out') args.out = path.resolve(argv[++i]);
  }
  return args;
}

function normalizeAddressKey(addr) {
  if (!addr) return null;
  return String(addr).toUpperCase().replace(/[.,#]/g, '').replace(/\s+/g, ' ').trim();
}

async function fetchPage(offset) {
  const params = new URLSearchParams({
    where: '1=1',
    outFields: OUT_FIELDS.join(','),
    resultOffset: String(offset),
    resultRecordCount: String(PAGE_SIZE),
    orderByFields: 'ObjectId',
    f: 'json',
  });
  const url = `${SERVICE_URL}/query?${params}`;
  for (let attempt = 0; attempt <= 3; attempt++) {
    const r = await fetch(url);
    if (r.ok) {
      const json = await r.json();
      if (json.error) throw new Error(`ArcGIS error: ${JSON.stringify(json.error)}`);
      return json;
    }
    if (r.status === 429 || r.status >= 500) {
      await new Promise((res) => setTimeout(res, Math.min(2 ** attempt, 20) * 1000));
      continue;
    }
    throw new Error(`HTTP ${r.status} fetching offset ${offset}`);
  }
  throw new Error(`Exhausted retries at offset ${offset}`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  await mkdir(args.out, { recursive: true });

  const records = [];
  let offset = 0;
  console.log('[export] fetching Property Standards Requests from Nashville ArcGIS...');
  while (true) {
    const page = await fetchPage(offset);
    const features = page.features || [];
    for (const f of features) {
      const a = f.attributes;
      // Match the SAME key shape as the eviction feed's address_formatted
      // ("STREET CITY STATE ZIP" once normalized) -- this service's own
      // Property_Address field is street-only, with city/state/zip in
      // separate columns, so they have to be joined back on to land on the
      // same key normalizeAddressKey() produces on the eviction side.
      const full = [a.Property_Address, a.City, a.State, a.ZIP].filter(Boolean).join(' ');
      records.push({
        addressKey: normalizeAddressKey(full),
        requestNbr: a.Request_Nbr,
        dateReceived: a.Date_Received, // epoch ms, per the service's own field type
        propertyApn: a.Property_APN,
        propertyAddress: a.Property_Address,
        propertyOwner: a.Property_Owner,
        complaintSource: a.Complaint_Source,
        reportedProblem: a.Reported_Problem,
        status: a.Status,
        cnclDist: a.Cncl_Dist,
        lastActivityDate: a.Last_Activity_Date,
        lastActivity: a.Last_Activity,
        lastActResult: a.Last_Act__Result,
        violationsNoted: a.Violations_Noted,
        lat: a.Lat,
        lon: a.Lon,
        zip: a.ZIP,
        subtypeDescription: a.Subtype_Description,
      });
    }
    console.log(`[export] ${records.length} records so far (offset ${offset})`);
    if (features.length < PAGE_SIZE) break; // last (partial or empty) page
    offset += PAGE_SIZE;
  }

  const withAddress = records.filter((r) => r.addressKey).length;
  const output = {
    exportedAt: new Date().toISOString(),
    source: {
      service: `${SERVICE_URL}/query`,
      dataset: 'Property Standards Requests',
      portalPage: 'https://data.nashville.gov/datasets/038de0cf3d35435c8c563b731265c036_0',
      note: 'Metro Codes\' own record of property standards / code-enforcement requests. '
        + 'Per the dataset\'s own description this covers a rolling three-year period, not '
        + 'full history -- it goes stale on Metro\'s schedule, independent of how recently '
        + 'this export ran.',
    },
    recordCount: records.length,
    recordsWithAddress: withAddress,
    violations: records,
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const finalFile = path.join(args.out, `code-violations-export-${stamp}.json`);
  await writeFile(finalFile, JSON.stringify(output, null, 2));
  console.log(`[export] done. ${records.length} records (${withAddress} with an address) written to ${finalFile}`);
}

main().catch((e) => {
  console.error('[export] fatal:', e);
  process.exit(1);
});
