import { isDeepStrictEqual } from 'node:util';

const clone = (value) => structuredClone(value);

export function getPath(document, path) {
  if (typeof path !== 'string' || !path.length) throw new TypeError('document path must be a non-empty string');
  const parts = path.split('.');
  return resolveValues(document, parts);
}

export function firstPathValue(document, path) {
  return getPath(document, path)[0];
}

export function matchesDocument(document, query = {}) {
  if (!query || typeof query !== 'object' || Array.isArray(query)) throw new TypeError('query must be an object');
  for (const [key, condition] of Object.entries(query)) {
    if (key === '$and') {
      if (!Array.isArray(condition) || !condition.every((part) => matchesDocument(document, part))) return false;
      continue;
    }
    if (key === '$or') {
      if (!Array.isArray(condition) || !condition.some((part) => matchesDocument(document, part))) return false;
      continue;
    }
    if (key === '$nor') {
      if (!Array.isArray(condition) || condition.some((part) => matchesDocument(document, part))) return false;
      continue;
    }
    if (key === '$not') {
      if (!condition || typeof condition !== 'object' || Array.isArray(condition) || matchesDocument(document, condition)) return false;
      continue;
    }
    if (key.startsWith('$')) throw new TypeError(`unsupported logical query operator: ${key}`);
    const values = getPath(document, key);
    if (!matchesCondition(values, condition)) return false;
  }
  return true;
}

export function projectDocument(document, projection) {
  if (projection === undefined || projection === null) return clone(document);
  if (!projection || typeof projection !== 'object' || Array.isArray(projection)) throw new TypeError('projection must be an object');
  const entries = Object.entries(projection);
  if (!entries.length) return clone(document);
  const includes = entries.filter(([, value]) => value === 1 || value === true).map(([key]) => key);
  const excludes = entries.filter(([, value]) => value === 0 || value === false).map(([key]) => key);
  const invalid = entries.filter(([, value]) => value !== 0 && value !== 1 && value !== false && value !== true);
  if (invalid.length) throw new TypeError('projection values must be boolean or 0/1');
  if (includes.length && excludes.some((key) => key !== 'id')) throw new TypeError('projection cannot mix inclusion and exclusion fields');

  if (includes.length) {
    const output = {};
    if (!excludes.includes('id') && Object.hasOwn(document, 'id')) output.id = clone(document.id);
    for (const field of includes) {
      if (field === 'id') continue;
      const value = readDirectPath(document, field);
      if (value.exists) writePath(output, field, clone(value.value));
    }
    return output;
  }

  const output = clone(document);
  for (const field of excludes) deletePath(output, field);
  return output;
}

export function applyDocumentUpdate(document, update) {
  if (!document || typeof document !== 'object' || Array.isArray(document)) throw new TypeError('document update requires an object document');
  if (!update || typeof update !== 'object' || Array.isArray(update)) throw new TypeError('document update requires an object update');
  const keys = Object.keys(update);
  const operatorKeys = keys.filter((key) => key.startsWith('$'));
  if (!operatorKeys.length) {
    if (update.id !== undefined && document.id !== undefined && update.id !== document.id) throw new Error('replacement update cannot change id');
    return clone({ ...update, id: document.id ?? update.id });
  }
  if (operatorKeys.length !== keys.length) throw new TypeError('update cannot mix replacement fields and operators');

  const result = clone(document);
  for (const [operator, changes] of Object.entries(update)) {
    if (!changes || typeof changes !== 'object' || Array.isArray(changes)) throw new TypeError(`${operator} requires an object`);
    if (operator === '$set') for (const [field, value] of Object.entries(changes)) setMutable(result, field, clone(value));
    else if (operator === '$unset') for (const field of Object.keys(changes)) deletePath(result, field);
    else if (operator === '$inc') for (const [field, amount] of Object.entries(changes)) numericUpdate(result, field, amount, (a, b) => a + b, '$inc');
    else if (operator === '$mul') for (const [field, amount] of Object.entries(changes)) numericUpdate(result, field, amount, (a, b) => a * b, '$mul');
    else if (operator === '$min') for (const [field, value] of Object.entries(changes)) compareUpdate(result, field, value, (current, candidate) => current === undefined || candidate < current);
    else if (operator === '$max') for (const [field, value] of Object.entries(changes)) compareUpdate(result, field, value, (current, candidate) => current === undefined || candidate > current);
    else if (operator === '$push') for (const [field, value] of Object.entries(changes)) arrayUpdate(result, field, value, false);
    else if (operator === '$addToSet') for (const [field, value] of Object.entries(changes)) arrayUpdate(result, field, value, true);
    else if (operator === '$pull') for (const [field, condition] of Object.entries(changes)) pullUpdate(result, field, condition);
    else if (operator === '$rename') for (const [from, to] of Object.entries(changes)) renamePath(result, from, to);
    else throw new TypeError(`unsupported update operator: ${operator}`);
  }
  if (document.id !== undefined && result.id !== document.id) throw new Error('document update cannot change id');
  return result;
}

export function aggregateDocuments(documents, pipeline = []) {
  if (!Array.isArray(pipeline)) throw new TypeError('aggregation pipeline must be an array');
  let rows = documents.map(clone);
  for (const stage of pipeline) {
    if (!stage || typeof stage !== 'object' || Array.isArray(stage) || Object.keys(stage).length !== 1) throw new TypeError('aggregation stage must contain exactly one operator');
    const [operator, spec] = Object.entries(stage)[0];
    if (operator === '$match') rows = rows.filter((row) => matchesDocument(row, spec));
    else if (operator === '$project') rows = rows.map((row) => projectDocument(row, spec));
    else if (operator === '$sort') rows = sortDocuments(rows, spec);
    else if (operator === '$skip') rows = rows.slice(validateNonNegativeInteger(spec, '$skip'));
    else if (operator === '$limit') rows = rows.slice(0, validateNonNegativeInteger(spec, '$limit'));
    else if (operator === '$count') rows = [{ [validateOutputField(spec)]: rows.length }];
    else if (operator === '$unwind') rows = unwindDocuments(rows, spec);
    else if (operator === '$group') rows = groupDocuments(rows, spec);
    else throw new TypeError(`unsupported aggregation stage: ${operator}`);
  }
  return rows;
}

function matchesCondition(values, condition) {
  const exists = values.length > 0;
  if (condition && typeof condition === 'object' && !Array.isArray(condition) && Object.keys(condition).some((key) => key.startsWith('$'))) {
    for (const [operator, expected] of Object.entries(condition)) {
      if (operator === '$exists') {
        if (Boolean(expected) !== exists) return false;
        continue;
      }
      if (operator === '$not') {
        if (matchesCondition(values, expected)) return false;
        continue;
      }
      const predicate = operatorPredicate(operator, expected);
      if (!values.some((value) => predicate(value))) return false;
    }
    return true;
  }
  return values.some((value) => equalMongoLike(value, condition));
}

function operatorPredicate(operator, expected) {
  if (operator === '$eq') return (value) => equalMongoLike(value, expected);
  if (operator === '$ne') return (value) => !equalMongoLike(value, expected);
  if (operator === '$gt') return (value) => value > expected;
  if (operator === '$gte') return (value) => value >= expected;
  if (operator === '$lt') return (value) => value < expected;
  if (operator === '$lte') return (value) => value <= expected;
  if (operator === '$in') {
    if (!Array.isArray(expected)) throw new TypeError('$in requires an array');
    return (value) => expected.some((candidate) => equalMongoLike(value, candidate));
  }
  if (operator === '$nin') {
    if (!Array.isArray(expected)) throw new TypeError('$nin requires an array');
    return (value) => !expected.some((candidate) => equalMongoLike(value, candidate));
  }
  if (operator === '$size') return (value) => Array.isArray(value) && value.length === expected;
  if (operator === '$all') {
    if (!Array.isArray(expected)) throw new TypeError('$all requires an array');
    return (value) => Array.isArray(value) && expected.every((candidate) => value.some((item) => equalMongoLike(item, candidate)));
  }
  if (operator === '$elemMatch') return (value) => Array.isArray(value) && value.some((item) => item && typeof item === 'object' && matchesDocument(item, expected));
  throw new TypeError(`unsupported query operator: ${operator}`);
}

function resolveValues(value, parts) {
  if (!parts.length) return [value];
  if (Array.isArray(value)) return value.flatMap((item) => resolveValues(item, parts));
  if (!value || typeof value !== 'object') return [];
  const [head, ...tail] = parts;
  if (!Object.hasOwn(value, head)) return [];
  return resolveValues(value[head], tail);
}

function equalMongoLike(actual, expected) {
  if (Array.isArray(actual) && !Array.isArray(expected)) return actual.some((item) => isDeepStrictEqual(item, expected));
  return isDeepStrictEqual(actual, expected);
}

function readDirectPath(document, path) {
  const parts = path.split('.');
  let current = document;
  for (const part of parts) {
    if (!current || typeof current !== 'object' || !Object.hasOwn(current, part)) return { exists: false, value: undefined };
    current = current[part];
  }
  return { exists: true, value: current };
}

function setMutable(document, path, value) {
  const parts = path.split('.');
  if (!parts.length || parts.some((part) => !part)) throw new TypeError('update path is invalid');
  let current = document;
  for (let index = 0; index < parts.length - 1; index++) {
    const part = parts[index];
    const existing = current[part];
    if (existing === undefined) current[part] = {};
    else if (!existing || typeof existing !== 'object' || Array.isArray(existing)) throw new TypeError(`cannot traverse non-object update path: ${path}`);
    current = current[part];
  }
  current[parts.at(-1)] = value;
}

function writePath(document, path, value) { setMutable(document, path, value); }

function deletePath(document, path) {
  const parts = path.split('.');
  let current = document;
  for (let index = 0; index < parts.length - 1; index++) {
    if (!current || typeof current !== 'object') return false;
    current = current[parts[index]];
  }
  if (!current || typeof current !== 'object') return false;
  return delete current[parts.at(-1)];
}

function numericUpdate(document, field, amount, operation, name) {
  if (typeof amount !== 'number' || !Number.isFinite(amount)) throw new TypeError(`${name} requires finite numeric values`);
  const current = readDirectPath(document, field);
  if (current.exists && (typeof current.value !== 'number' || !Number.isFinite(current.value))) throw new TypeError(`${name} target must be numeric`);
  setMutable(document, field, operation(current.exists ? current.value : 0, amount));
}

function compareUpdate(document, field, candidate, predicate) {
  const current = readDirectPath(document, field);
  if (predicate(current.exists ? current.value : undefined, candidate)) setMutable(document, field, clone(candidate));
}

function arrayUpdate(document, field, value, unique) {
  const current = readDirectPath(document, field);
  let array;
  if (!current.exists) array = [];
  else if (Array.isArray(current.value)) array = clone(current.value);
  else throw new TypeError(`${unique ? '$addToSet' : '$push'} target must be an array`);
  const values = value && typeof value === 'object' && !Array.isArray(value) && Array.isArray(value.$each) ? value.$each : [value];
  for (const item of values) if (!unique || !array.some((existing) => isDeepStrictEqual(existing, item))) array.push(clone(item));
  setMutable(document, field, array);
}

function pullUpdate(document, field, condition) {
  const current = readDirectPath(document, field);
  if (!current.exists) return;
  if (!Array.isArray(current.value)) throw new TypeError('$pull target must be an array');
  const retained = current.value.filter((item) => !pullMatches(item, condition));
  setMutable(document, field, retained);
}

function pullMatches(item, condition) {
  if (!condition || typeof condition !== 'object' || Array.isArray(condition)) return equalMongoLike(item, condition);
  const keys = Object.keys(condition);
  if (keys.some((key) => key.startsWith('$'))) return matchesCondition([item], condition);
  if (item && typeof item === 'object' && !Array.isArray(item)) return matchesDocument(item, condition);
  return equalMongoLike(item, condition);
}

function renamePath(document, from, to) {
  if (typeof to !== 'string' || !to.length) throw new TypeError('$rename target must be a non-empty path');
  if (from === 'id' || to === 'id') throw new Error('$rename cannot move id');
  const current = readDirectPath(document, from);
  if (!current.exists) return;
  deletePath(document, from);
  setMutable(document, to, clone(current.value));
}

function sortDocuments(rows, spec) {
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) throw new TypeError('$sort requires an object');
  const entries = Object.entries(spec);
  if (!entries.length || entries.some(([, direction]) => direction !== 1 && direction !== -1)) throw new TypeError('$sort directions must be 1 or -1');
  return [...rows].sort((left, right) => {
    for (const [field, direction] of entries) {
      const a = firstPathValue(left, field);
      const b = firstPathValue(right, field);
      if (isDeepStrictEqual(a, b)) continue;
      if (a === undefined) return -1 * direction;
      if (b === undefined) return 1 * direction;
      return (a > b ? 1 : -1) * direction;
    }
    return 0;
  });
}

function unwindDocuments(rows, spec) {
  const field = typeof spec === 'string' ? spec : spec?.path;
  if (typeof field !== 'string' || !field.length) throw new TypeError('$unwind requires a path');
  const normalized = field.startsWith('$') ? field.slice(1) : field;
  const output = [];
  for (const row of rows) {
    const direct = readDirectPath(row, normalized);
    if (!direct.exists || !Array.isArray(direct.value)) continue;
    for (const item of direct.value) {
      const copy = clone(row);
      setMutable(copy, normalized, clone(item));
      output.push(copy);
    }
  }
  return output;
}

function groupDocuments(rows, spec) {
  if (!spec || typeof spec !== 'object' || Array.isArray(spec) || !Object.hasOwn(spec, '_id')) throw new TypeError('$group requires _id');
  const groups = new Map();
  for (const row of rows) {
    const id = evaluateExpression(row, spec._id);
    const key = JSON.stringify(id);
    let group = groups.get(key);
    if (!group) { group = { _id: clone(id), __rows: [] }; groups.set(key, group); }
    group.__rows.push(row);
  }
  return [...groups.values()].map((group) => {
    const output = { _id: group._id };
    for (const [field, expression] of Object.entries(spec)) {
      if (field === '_id') continue;
      if (!expression || typeof expression !== 'object' || Array.isArray(expression) || Object.keys(expression).length !== 1) throw new TypeError('group accumulator must contain one operator');
      const [operator, operand] = Object.entries(expression)[0];
      const values = group.__rows.map((row) => evaluateExpression(row, operand));
      if (operator === '$sum') output[field] = values.reduce((sum, value) => sum + (typeof value === 'number' ? value : 0), 0);
      else if (operator === '$avg') { const nums = values.filter((value) => typeof value === 'number'); output[field] = nums.length ? nums.reduce((a,b)=>a+b,0)/nums.length : null; }
      else if (operator === '$min') output[field] = values.reduce((best, value) => best === undefined || value < best ? value : best, undefined);
      else if (operator === '$max') output[field] = values.reduce((best, value) => best === undefined || value > best ? value : best, undefined);
      else if (operator === '$first') output[field] = clone(values[0]);
      else if (operator === '$last') output[field] = clone(values.at(-1));
      else if (operator === '$push') output[field] = values.map(clone);
      else if (operator === '$addToSet') output[field] = values.reduce((set, value) => { if (!set.some((existing) => isDeepStrictEqual(existing, value))) set.push(clone(value)); return set; }, []);
      else throw new TypeError(`unsupported group accumulator: ${operator}`);
    }
    return output;
  });
}

function evaluateExpression(row, expression) {
  if (typeof expression === 'string' && expression.startsWith('$')) return firstPathValue(row, expression.slice(1));
  return clone(expression);
}

function validateNonNegativeInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} requires a non-negative safe integer`);
  return value;
}

function validateOutputField(value) {
  if (typeof value !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) throw new TypeError('$count requires a valid output field name');
  return value;
}
