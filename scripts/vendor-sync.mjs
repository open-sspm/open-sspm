#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const vendorDir = path.join(rootDir, "web/static/vendor");
const manifestPath = path.join(vendorDir, "vendor.manifest.json");
const allowMissingManifest = process.argv.includes("--allow-missing-manifest");

const fail = (message) => {
  console.error(`vendor:sync: ${message}`);
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

const readBufferIfExists = async (filePath) => {
  try {
    return await readFile(filePath);
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
};

const writeFileAtomic = async (destinationPath, buffer) => {
  const destinationDir = path.dirname(destinationPath);
  const tempPath = path.join(
    destinationDir,
    `.${path.basename(destinationPath)}.tmp-${process.pid}-${Date.now()}`,
  );

  try {
    await writeFile(tempPath, buffer);
    await rename(tempPath, destinationPath);
  } catch (error) {
    await rm(tempPath, { force: true }).catch(() => {});
    throw error;
  }
};

const readManifest = async () => {
  if (!existsSync(manifestPath)) {
    if (allowMissingManifest) {
      console.warn(`vendor:sync: skipping (manifest not found at ${manifestPath})`);
      process.exit(0);
    }
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

const pruneUnmanaged = async (managedDestinationPaths) => {
  const entries = await readdir(vendorDir, { withFileTypes: true });
  const removed = [];

  for (const entry of entries) {
    if (!entry.isFile()) continue;
    if (!entry.name.endsWith(".js")) continue;

    const fullPath = path.join(vendorDir, entry.name);
    if (managedDestinationPaths.has(fullPath)) continue;

    await rm(fullPath);
    removed.push(path.relative(rootDir, fullPath));
  }

  return removed;
};

const main = async () => {
  const assets = await readManifest();
  const managedDestinationPaths = new Set();
  let changedCount = 0;
  let unchangedCount = 0;

  for (const asset of assets) {
    const packageName = String(asset.package || "").trim();
    const sourceRel = String(asset.source || "").trim();
    const destinationRel = String(asset.destination || "").trim();
    if (!packageName || !sourceRel || !destinationRel) {
      fail("each manifest asset must include package, source, and destination");
    }

    const sourcePath = path.join(rootDir, "node_modules", packageName, sourceRel);
    const destinationPath = ensureSafeDestination(destinationRel);
    managedDestinationPaths.add(destinationPath);

    const sourceBuffer = await readBufferIfExists(sourcePath);
    if (!sourceBuffer) {
      fail(`source file not found: ${path.relative(rootDir, sourcePath)}`);
    }

    const destinationBuffer = await readBufferIfExists(destinationPath);
    const sameContent =
      destinationBuffer !== null && digest(sourceBuffer) === digest(destinationBuffer);

    if (sameContent) {
      unchangedCount += 1;
      continue;
    }

    await mkdir(path.dirname(destinationPath), { recursive: true });
    await writeFileAtomic(destinationPath, sourceBuffer);
    changedCount += 1;
  }

  const removed = await pruneUnmanaged(managedDestinationPaths);
  const removedCount = removed.length;

  console.log(
    `vendor:sync: ${changedCount} updated, ${unchangedCount} unchanged, ${removedCount} removed`,
  );
  for (const removedPath of removed) {
    console.log(`vendor:sync: removed ${removedPath}`);
  }
};

main().catch((error) => {
  fail(error.message);
});
