/**
 * Schema Evolution - Safe API versioning for GLYPH
 *
 * Enables schemas to evolve safely without breaking clients. Supports:
 * - Adding new optional fields
 * - Renaming fields (with compatibility mapping)
 * - Deprecating fields
 * - Changing defaults
 * - Strict vs tolerant parsing modes
 */

// ============================================================
// Evolution Mode
// ============================================================

export enum EvolutionMode {
  /** Fail on unknown fields */
  Strict = 'strict',
  /** Ignore unknown fields (default) */
  Tolerant = 'tolerant',
  /** Auto-migrate between versions */
  Migrate = 'migrate',
}

// ============================================================
// Field Types
// ============================================================

export type FieldType = 'str' | 'int' | 'float' | 'bool' | 'list' | 'decimal';

export type FieldValue = null | boolean | number | string | FieldValue[];

// ============================================================
// Evolving Field
// ============================================================

export interface EvolvingFieldConfig {
  type: FieldType;
  required?: boolean;
  default?: FieldValue;
  addedIn?: string;
  deprecatedIn?: string;
  renamedFrom?: string;
  validation?: string | RegExp;
}

export class EvolvingField {
  readonly name: string;
  readonly type: FieldType;
  readonly required: boolean;
  readonly default: FieldValue | undefined;
  readonly addedIn: string;
  readonly deprecatedIn: string | undefined;
  readonly renamedFrom: string | undefined;
  readonly validation: RegExp | undefined;

  constructor(name: string, config: EvolvingFieldConfig) {
    this.name = name;
    this.type = config.type;
    this.required = config.required ?? false;
    this.default = config.default;
    this.addedIn = config.addedIn ?? '1.0';
    this.deprecatedIn = config.deprecatedIn;
    this.renamedFrom = config.renamedFrom;
    this.validation = config.validation
      ? (typeof config.validation === 'string' ? new RegExp(config.validation) : config.validation)
      : undefined;
  }

  /**
   * Check if field is available in a given version.
   */
  isAvailableIn(version: string): boolean {
    if (compareVersions(version, this.addedIn) < 0) {
      return false;
    }

    if (this.deprecatedIn && compareVersions(version, this.deprecatedIn) >= 0) {
      return false;
    }

    return true;
  }

  /**
   * Check if field is deprecated in a given version.
   */
  isDeprecatedIn(version: string): boolean {
    if (!this.deprecatedIn) {
      return false;
    }
    return compareVersions(version, this.deprecatedIn) >= 0;
  }

  /**
   * Validate a value against this field.
   */
  validate(value: FieldValue): string | null {
    if (value === null || value === undefined) {
      if (this.required) {
        return `field ${this.name} is required`;
      }
      return null;
    }

    // Type checking
    switch (this.type) {
      case 'str':
        if (typeof value !== 'string') {
          return `field ${this.name} must be string`;
        }
        if (this.validation && !this.validation.test(value)) {
          return `field ${this.name} does not match pattern`;
        }
        break;
      case 'int':
        if (typeof value !== 'number' || !Number.isInteger(value)) {
          return `field ${this.name} must be int`;
        }
        break;
      case 'float':
        if (typeof value !== 'number') {
          return `field ${this.name} must be float`;
        }
        break;
      case 'bool':
        if (typeof value !== 'boolean') {
          return `field ${this.name} must be bool`;
        }
        break;
      case 'list':
        if (!Array.isArray(value)) {
          return `field ${this.name} must be list`;
        }
        break;
    }

    return null;
  }
}

// ============================================================
// Version Schema
// ============================================================

export class VersionSchema {
  readonly name: string;
  readonly version: string;
  readonly fields: Map<string, EvolvingField>;
  description: string;

  constructor(name: string, version: string) {
    this.name = name;
    this.version = version;
    this.fields = new Map();
    this.description = '';
  }

  /**
   * Add a field.
   */
  addField(field: EvolvingField): void {
    this.fields.set(field.name, field);
  }

  /**
   * Get a field by name.
   */
  getField(name: string): EvolvingField | undefined {
    return this.fields.get(name);
  }

  /**
   * Validate data against this schema.
   */
  validate(data: Record<string, FieldValue>): string | null {
    // Check required fields
    for (const [name, field] of this.fields) {
      if (field.required && !(name in data)) {
        return `missing required field: ${name}`;
      }
    }

    // Validate field values
    for (const [name, value] of Object.entries(data)) {
      const field = this.fields.get(name);
      if (field) {
        const error = field.validate(value);
        if (error) {
          return error;
        }
      }
    }

    return null;
  }
}

// ============================================================
// Versioned Schema
// ============================================================

export interface ParseResult {
  error?: string;
  data?: Record<string, FieldValue>;
}

export interface EmitResult {
  error?: string;
  header?: string;
}

export interface ChangelogEntry {
  version: string;
  description: string;
  addedFields: string[];
  deprecatedFields: string[];
  renamedFields: [string, string][];
}

export class VersionedSchema {
  readonly name: string;
  readonly versions: Map<string, VersionSchema>;
  latestVersion: string;
  mode: EvolutionMode;

  constructor(name: string) {
    this.name = name;
    this.versions = new Map();
    this.latestVersion = '1.0';
    this.mode = EvolutionMode.Tolerant;
  }

  /**
   * Set evolution mode.
   */
  withMode(mode: EvolutionMode): this {
    this.mode = mode;
    return this;
  }

  /**
   * Add a version with fields.
   */
  addVersion(version: string, fields: Record<string, EvolvingFieldConfig>): void {
    const schema = new VersionSchema(this.name, version);

    for (const [name, config] of Object.entries(fields)) {
      const fieldConfig = { ...config };
      if (!fieldConfig.addedIn) {
        fieldConfig.addedIn = version;
      }
      schema.addField(new EvolvingField(name, fieldConfig));
    }

    this.versions.set(version, schema);
    this.latestVersion = this.getLatestVersion();
  }

  /**
   * Get schema for a specific version.
   */
  getVersion(version: string): VersionSchema | undefined {
    return this.versions.get(version);
  }

  /**
   * Parse data from a specific version.
   */
  parse(data: Record<string, FieldValue>, fromVersion: string): ParseResult {
    const schema = this.getVersion(fromVersion);
    if (!schema) {
      return { error: `unknown version: ${fromVersion}` };
    }

    // Validate in strict mode
    if (this.mode === EvolutionMode.Strict) {
      const error = schema.validate(data);
      if (error) {
        return { error };
      }
    }

    let result = { ...data };

    // Migrate to latest if needed
    if (fromVersion !== this.latestVersion) {
      const migrated = this.migrate(data, fromVersion, this.latestVersion);
      if (migrated.error) {
        return migrated;
      }
      result = migrated.data!;
    }

    // Filter unknown fields in tolerant mode
    if (this.mode === EvolutionMode.Tolerant) {
      const targetSchema = this.getVersion(this.latestVersion);
      if (targetSchema) {
        const filtered: Record<string, FieldValue> = {};
        for (const [k, v] of Object.entries(result)) {
          if (targetSchema.fields.has(k)) {
            filtered[k] = v;
          }
        }
        result = filtered;
      }
    }

    return { data: result };
  }

  /**
   * Emit version header for data.
   */
  emit(data: Record<string, FieldValue>, version?: string): EmitResult {
    const targetVersion = version ?? this.latestVersion;

    const schema = this.getVersion(targetVersion);
    if (!schema) {
      return { error: `unknown version: ${targetVersion}` };
    }

    const error = schema.validate(data);
    if (error) {
      return { error };
    }

    return { header: `@version ${targetVersion}` };
  }

  /**
   * Migrate data between versions.
   */
  private migrate(
    data: Record<string, FieldValue>,
    fromVersion: string,
    toVersion: string
  ): ParseResult {
    const path = this.getMigrationPath(fromVersion, toVersion);
    if (!path) {
      return { error: `cannot migrate from ${fromVersion} to ${toVersion}` };
    }

    let currentData = { ...data };
    let currentVersion = fromVersion;

    for (const nextVersion of path) {
      const result = this.migrateStep(currentData, currentVersion, nextVersion);
      if (result.error) {
        return result;
      }
      currentData = result.data!;
      currentVersion = nextVersion;
    }

    return { data: currentData };
  }

  /**
   * Migrate one step.
   */
  private migrateStep(
    data: Record<string, FieldValue>,
    _fromVersion: string,
    toVersion: string
  ): ParseResult {
    const toSchema = this.getVersion(toVersion);
    if (!toSchema) {
      return { error: 'invalid version' };
    }

    const result = { ...data };

    // Handle field renames
    for (const [name, field] of toSchema.fields) {
      if (field.renamedFrom && field.renamedFrom in result && !(name in result)) {
        result[name] = result[field.renamedFrom];
        delete result[field.renamedFrom];
      }
    }

    // Handle new fields with defaults
    for (const [name, field] of toSchema.fields) {
      if (!(name in result)) {
        if (field.default !== undefined) {
          result[name] = field.default;
        } else if (!field.required) {
          result[name] = null;
        }
      }
    }

    // Remove unknown fields in tolerant mode
    if (this.mode === EvolutionMode.Tolerant) {
      for (const key of Object.keys(result)) {
        if (!toSchema.fields.has(key)) {
          delete result[key];
        }
      }
    }

    return { data: result };
  }

  /**
   * Get migration path between versions.
   */
  private getMigrationPath(fromVersion: string, toVersion: string): string[] | null {
    const versions = Array.from(this.versions.keys()).sort(
      (a, b) => compareVersions(a, b)
    );

    const fromIdx = versions.indexOf(fromVersion);
    const toIdx = versions.indexOf(toVersion);

    if (fromIdx === -1 || toIdx === -1) {
      return null;
    }

    if (fromIdx < toIdx) {
      return versions.slice(fromIdx + 1, toIdx + 1);
    } else if (fromIdx > toIdx) {
      return null; // Downgrade not supported
    }

    return [];
  }

  /**
   * Get the latest version string.
   */
  private getLatestVersion(): string {
    const versions = Array.from(this.versions.keys()).sort(
      (a, b) => compareVersions(a, b)
    );
    return versions[versions.length - 1] ?? '1.0';
  }

  /**
   * Get changelog of schema evolution.
   */
  getChangelog(): ChangelogEntry[] {
    const versions = Array.from(this.versions.keys()).sort(
      (a, b) => compareVersions(a, b)
    );

    return versions.map(version => {
      const schema = this.versions.get(version)!;

      const addedFields: string[] = [];
      const deprecatedFields: string[] = [];
      const renamedFields: [string, string][] = [];

      for (const [name, field] of schema.fields) {
        if (field.addedIn === version) {
          addedFields.push(name);
        }
        if (field.deprecatedIn === version) {
          deprecatedFields.push(name);
        }
        if (field.renamedFrom) {
          renamedFields.push([field.renamedFrom, name]);
        }
      }

      return {
        version,
        description: schema.description,
        addedFields,
        deprecatedFields,
        renamedFields,
      };
    });
  }
}

// ============================================================
// Helper Functions
// ============================================================

/**
 * Compare two version strings.
 * Returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
 */
export function compareVersions(v1: string, v2: string): number {
  const parts1 = v1.split('.').map(s => parseInt(s, 10) || 0);
  const parts2 = v2.split('.').map(s => parseInt(s, 10) || 0);

  const maxLen = Math.max(parts1.length, parts2.length);
  for (let i = 0; i < maxLen; i++) {
    const p1 = parts1[i] ?? 0;
    const p2 = parts2[i] ?? 0;

    if (p1 < p2) return -1;
    if (p1 > p2) return 1;
  }

  return 0;
}

/**
 * Parse a version header (e.g., "@version 2.0").
 */
export function parseVersionHeader(text: string): string | null {
  text = text.trim();
  if (!text.startsWith('@version ')) {
    return null;
  }
  const version = text.slice(9).trim();
  if (!version) {
    return null;
  }
  return version;
}

/**
 * Format a version header.
 */
export function formatVersionHeader(version: string): string {
  return `@version ${version}`;
}

/**
 * Create a versioned schema.
 */
export function versionedSchema(name: string): VersionedSchema {
  return new VersionedSchema(name);
}
