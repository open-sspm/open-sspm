#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const vendorDir = path.join(rootDir, "web/static/vendor");
const manifestPath = path.join(vendorDir, "vendor.manifest.json");

const fail = (message) => {
  console.error(`vendor:check: ${message}`);
  process.exit(1);
};

const digest = (buffer) => createHash("sha256").update(buffer).digest("hex");

const ensureSafeDestination = (destination) => {
  const resolved = path.resolve(rootDir, destination);
  if (!resolved.startsWith(`${vendorDir}${path.sep}`)) {
    fail(`destination must stay under web/static/vendor: ${destination}`);
  }
  return resolved;
};

const readManifest = async () => {
  if (!existsSync(manifestPath)) {
    fail(`manifest not found at ${manifestPath}`);
  }

  let raw;
  try {
    raw = await readFile(manifestPath, "utf8");
  } catch (error) {
    fail(`cannot read manifest: ${error.message}`);
  }

  let manifest;
  try {
    manifest = JSON.parse(raw);
  } catch (error) {
    fail(`invalid manifest JSON: ${error.message}`);
  }

  if (!Array.isArray(manifest.assets) || manifest.assets.length === 0) {
    fail("manifest must contain a non-empty assets array");
  }

  return manifest.assets;
};

const main = async () => {
  const assets = await readManifest();
  const failures = [];
  const managedDestinationPaths = new Set();

  for (const asset of assets) {
    const packageName = String(asset.package || "").trim();
    const sourceRel = String(asset.source || "").trim();
    const destinationRel = String(asset.destination || "").trim();

    if (!packageName || !sourceRel || !destinationRel) {
      failures.push("manifest contains an asset missing package/source/destination");
      continue;
    }

    const sourcePath = path.join(rootDir, "node_modules", packageName, sourceRel);
    const destinationPath = ensureSafeDestination(destinationRel);
    managedDestinationPaths.add(destinationPath);

    if (!existsSync(sourcePath)) {
      failures.push(`missing source: ${path.relative(rootDir, sourcePath)}`);
      continue;
    }
    if (!existsSync(destinationPath)) {
      failures.push(`missing vendored file: ${path.relative(rootDir, destinationPath)}`);
      continue;
    }

    const [sourceBuffer, destinationBuffer] = await Promise.all([
      readFile(sourcePath),
      readFile(destinationPath),
    ]);

    if (digest(sourceBuffer) !== digest(destinationBuffer)) {
      failures.push(`out of sync: ${path.relative(rootDir, destinationPath)}`);
    }
  }

  const entries = await readdir(vendorDir, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    if (!entry.name.endsWith(".js")) continue;

    const fullPath = path.join(vendorDir, entry.name);
    if (!managedDestinationPaths.has(fullPath)) {
      failures.push(`unmanaged vendor JS: ${path.relative(rootDir, fullPath)}`);
    }
  }

  if (failures.length > 0) {
    console.error("vendor:check: failed:");
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    console.error("run `npm run vendor:sync` to repair managed files.");
    process.exit(1);
  }

  console.log(`vendor:check: OK (${assets.length} managed files)`);
};

main().catch((error) => {
  fail(error.message);
});
