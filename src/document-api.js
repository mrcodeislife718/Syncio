import { applyDocumentUpdate, aggregateDocuments } from './document.js';

export async function atomicUpdateDocument(db, collectionName, id, update, { upsert = false } = {}) {
  if (!db || typeof db.transaction !== 'function') throw new TypeError('atomicUpdateDocument requires a transactional Syncio database');
  if (typeof collectionName !== 'string' || !collectionName.length) throw new TypeError('collectionName is required');
  if (typeof id !== 'string' || !id.length) throw new TypeError('document id is required');
  let updated;
  await db.transaction(async (tx) => {
    const collection = tx.collection(collectionName);
    const current = collection.get(id);
    if (!current && !upsert) {
      const error = new Error(`document not found: ${id}`);
      error.code = 'SYNCIO_DOCUMENT_NOT_FOUND';
      error.statusCode = 404;
      throw error;
    }
    const base = current ?? { id };
    updated = applyDocumentUpdate(base, update);
    if (!updated.id) updated.id = id;
    if (updated.id !== id) throw new Error('atomic update cannot change document id');
    collection.put(updated);
  });
  return structuredClone(updated);
}

export function aggregateCollection(db, collectionName, pipeline) {
  if (!db || typeof db.collection !== 'function') throw new TypeError('aggregateCollection requires a Syncio database');
  return aggregateDocuments(db.collection(collectionName).all(), pipeline);
}
