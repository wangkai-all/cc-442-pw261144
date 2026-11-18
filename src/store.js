import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * JSON-file snapshot store. Writes are atomic (tmp file + rename) so a crash
 * mid-save cannot corrupt the previous snapshot.
 */
export class JsonStore {
  constructor(file) {
    this.file = file;
  }
  load() {
    if (!this.file || !existsSync(this.file)) return null;
    try {
      return JSON.parse(readFileSync(this.file, 'utf8'));
    } catch {
      return null; // corrupt snapshot: start clean rather than crash
    }
  }
  save(state) {
    if (!this.file) return;
    mkdirSync(dirname(this.file), { recursive: true });
    const tmp = `${this.file}.tmp`;
    writeFileSync(tmp, JSON.stringify(state, null, 2));
    renameSync(tmp, this.file);
  }
}
