import crypto from 'node:crypto';

const stable = value => {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${stable(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
};

export function createCommitFabric({ databaseId, partitionId = 'local', sequence, logicalTime = Date.now(), transactionId = null, origin = 'local', mutations = [], schemaVersion = 1, policyVersion = 1, causalParents = [] }) {
  if (!databaseId || !Number.isSafeInteger(sequence) || sequence < 1) throw new TypeError('invalid commit identity');
  const body = { version: 1, databaseId, partitionId, sequence, logicalTime, transactionId, origin, mutations: structuredClone(mutations), schemaVersion, policyVersion, causalParents: [...new Set(causalParents)].sort() };
  const checksum = crypto.createHash('sha256').update(stable(body)).digest('hex');
  const commitId = `${databaseId}:${partitionId}:${sequence}:${checksum.slice(0,16)}`;
  return Object.freeze({ ...body, commitId, checksum });
}

export function verifyCommitFabric(commit) {
  if (!commit || commit.version !== 1) return false;
  const { commitId, checksum, ...body } = commit;
  const expected = crypto.createHash('sha256').update(stable(body)).digest('hex');
  return checksum === expected && commitId === `${body.databaseId}:${body.partitionId}:${body.sequence}:${expected.slice(0,16)}`;
}
