import fs from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const root = path.resolve(new URL('..', import.meta.url).pathname);
const packageJson = JSON.parse(await fs.readFile(path.join(root, 'package.json'), 'utf8'));
const targets = Object.values(packageJson.exports ?? {});
if (!targets.length) throw new Error('package exports must not be empty');
if (new Set(targets).size !== targets.length) throw new Error('package exports must not contain duplicate targets');

for (const target of targets) {
  if (typeof target !== 'string' || !target.startsWith('./')) throw new Error(`invalid package export target: ${target}`);
  const file = path.resolve(root, target);
  const stat = await fs.stat(file).catch(() => null);
  if (!stat?.isFile()) throw new Error(`package export target is missing: ${target}`);
  const module = await import(`${pathToFileURL(file).href}?public-api-check=1`);
  if (Object.keys(module).length === 0) throw new Error(`package export has no public symbols: ${target}`);
}

for (const [name, target] of Object.entries(packageJson.bin ?? {})) {
  if (typeof target !== 'string') throw new Error(`invalid bin target for ${name}`);
  const file = path.resolve(root, target);
  const stat = await fs.stat(file).catch(() => null);
  if (!stat?.isFile()) throw new Error(`bin target is missing: ${name} -> ${target}`);
}

console.log(JSON.stringify({ ok: true, exports: targets.length, bins: Object.keys(packageJson.bin ?? {}).length }));
