'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');

const DEFAULT_PRICE_API_URL =
  'https://script.google.com/macros/s/AKfycbyt69e_-lJFz5q_d1TBCRlLqL9Hy3LD2_8GOPACU1nI_3Znvr_japJkzYNyqu4d250I/exec';
const ALLOWED_CONDITIONS = ['slidt', 'brugt', 'velholdt', 'perfekt'];
const RETRY_DELAYS_MS = [0, 750, 1500, 3000];
const REQUEST_TIMEOUT_MS = 20000;
const MAX_CONCURRENCY = 4;

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function endpoint(baseUrl, params) {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  return url;
}

async function fetchJsonWithRetry(url, fetchImpl = fetch) {
  let lastError;

  for (const delay of RETRY_DELAYS_MS) {
    if (delay) await wait(delay);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const response = await fetchImpl(url, {
        headers: { accept: 'application/json' },
        redirect: 'follow',
        signal: controller.signal,
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      if (!payload || payload.ok !== true) {
        throw new Error(payload?.error || payload?.message || 'Invalid price response');
      }
      return payload;
    } catch (error) {
      lastError = error;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError || new Error('Price request failed');
}

async function mapConcurrent(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await mapper(items[index], index);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => worker()),
  );
  return results;
}

function sanitizeConditions(conditions) {
  const byKey = new Map();

  for (const condition of Array.isArray(conditions) ? conditions : []) {
    const key = String(condition?.key || condition?.label || '').trim().toLowerCase();
    const price = Number(condition?.price);
    if (!ALLOWED_CONDITIONS.includes(key) || !Number.isFinite(price) || price <= 0) continue;
    byKey.set(key, {
      key,
      label: String(condition?.label || key).trim(),
      price,
    });
  }

  return ALLOWED_CONDITIONS.map((key) => byKey.get(key)).filter(Boolean);
}

function validateSnapshot(snapshot, sourceSite) {
  if (!snapshot || snapshot.ok !== true || !Array.isArray(snapshot.models)) {
    throw new Error(`${sourceSite}: snapshot is invalid`);
  }
  if (snapshot.models.length < 10) {
    throw new Error(`${sourceSite}: snapshot contains too few models`);
  }

  let storageCount = 0;
  for (const model of snapshot.models) {
    if (!model.name || !Array.isArray(model.storages) || !model.storages.length) {
      throw new Error(`${sourceSite}: ${model.name || 'unknown model'} has no storage variants`);
    }
    for (const storage of model.storages) {
      storageCount += 1;
      if (!storage.key || storage.conditions.length !== ALLOWED_CONDITIONS.length) {
        throw new Error(`${sourceSite}: ${model.name} ${storage.key || ''} has incomplete prices`);
      }
    }
  }

  if (storageCount < 20) throw new Error(`${sourceSite}: snapshot contains too few storage variants`);
  return snapshot;
}

async function buildSnapshot(sourceSite, options = {}) {
  const baseUrl = options.baseUrl || process.env.PRICE_API_URL || DEFAULT_PRICE_API_URL;
  const fetchImpl = options.fetchImpl || fetch;
  const catalog = await fetchJsonWithRetry(endpoint(baseUrl, { sourceSite }), fetchImpl);
  const variants = [];

  for (const model of catalog.models || []) {
    for (const storage of model.storages || []) {
      variants.push({ model, storage });
    }
  }

  const pricedVariants = await mapConcurrent(
    variants,
    options.concurrency || MAX_CONCURRENCY,
    async ({ model, storage }) => {
      const quote = await fetchJsonWithRetry(
        endpoint(baseUrl, {
          action: 'quote',
          sourceSite,
          model: model.name,
          storage: storage.key,
        }),
        fetchImpl,
      );
      return {
        modelName: model.name,
        storage: {
          key: String(storage.key),
          label: String(storage.label || storage.key),
          conditions: sanitizeConditions(quote.conditions),
        },
      };
    },
  );

  const storageByModel = new Map();
  pricedVariants.forEach(({ modelName, storage }) => {
    if (!storageByModel.has(modelName)) storageByModel.set(modelName, []);
    storageByModel.get(modelName).push(storage);
  });

  const snapshot = {
    ok: true,
    version: 1,
    updatedAt: String(catalog.priceListUpdatedAt || ''),
    models: (catalog.models || []).map((model) => ({
      name: String(model.name),
      key: String(model.key || ''),
      storages: storageByModel.get(model.name) || [],
    })),
  };

  return validateSnapshot(snapshot, sourceSite);
}

async function writeSnapshots(options = {}) {
  const outputDir = options.outputDir || path.join(__dirname, '..', 'price-cache');
  const [zrep, phoneparts] = await Promise.all([
    buildSnapshot('zrep', options),
    buildSnapshot('phoneparts', options),
  ]);

  await fs.mkdir(outputDir, { recursive: true });
  await Promise.all([
    fs.writeFile(path.join(outputDir, 'zrep.json'), `${JSON.stringify(zrep, null, 2)}\n`, 'utf8'),
    fs.writeFile(
      path.join(outputDir, 'phoneparts.json'),
      `${JSON.stringify(phoneparts, null, 2)}\n`,
      'utf8',
    ),
  ]);

  return {
    zrep: { models: zrep.models.length },
    phoneparts: { models: phoneparts.models.length },
  };
}

if (require.main === module) {
  writeSnapshots()
    .then((result) => console.log(JSON.stringify({ ok: true, ...result })))
    .catch((error) => {
      console.error(error?.stack || error);
      process.exitCode = 1;
    });
}

module.exports = {
  ALLOWED_CONDITIONS,
  buildSnapshot,
  fetchJsonWithRetry,
  sanitizeConditions,
  validateSnapshot,
  writeSnapshots,
};
