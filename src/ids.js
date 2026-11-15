import { randomUUID } from 'node:crypto';

export function newId(prefix) {
  return `${prefix}_${randomUUID().replaceAll('-', '').slice(0, 20)}`;
}

export const NEW_ID_PREFIX = 'id';
