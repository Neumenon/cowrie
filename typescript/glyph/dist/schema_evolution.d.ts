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
export declare enum EvolutionMode {
    /** Fail on unknown fields */
    Strict = "strict",
    /** Ignore unknown fields (default) */
    Tolerant = "tolerant",
    /** Auto-migrate between versions */
    Migrate = "migrate"
}
export type FieldType = 'str' | 'int' | 'float' | 'bool' | 'list' | 'decimal';
export type FieldValue = null | boolean | number | string | FieldValue[];
export interface EvolvingFieldConfig {
    type: FieldType;
    required?: boolean;
    default?: FieldValue;
    addedIn?: string;
    deprecatedIn?: string;
    renamedFrom?: string;
    validation?: string | RegExp;
}
export declare class EvolvingField {
    readonly name: string;
    readonly type: FieldType;
    readonly required: boolean;
    readonly default: FieldValue | undefined;
    readonly addedIn: string;
    readonly deprecatedIn: string | undefined;
    readonly renamedFrom: string | undefined;
    readonly validation: RegExp | undefined;
    constructor(name: string, config: EvolvingFieldConfig);
    /**
     * Check if field is available in a given version.
     */
    isAvailableIn(version: string): boolean;
    /**
     * Check if field is deprecated in a given version.
     */
    isDeprecatedIn(version: string): boolean;
    /**
     * Validate a value against this field.
     */
    validate(value: FieldValue): string | null;
}
export declare class VersionSchema {
    readonly name: string;
    readonly version: string;
    readonly fields: Map<string, EvolvingField>;
    description: string;
    constructor(name: string, version: string);
    /**
     * Add a field.
     */
    addField(field: EvolvingField): void;
    /**
     * Get a field by name.
     */
    getField(name: string): EvolvingField | undefined;
    /**
     * Validate data against this schema.
     */
    validate(data: Record<string, FieldValue>): string | null;
}
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
export declare class VersionedSchema {
    readonly name: string;
    readonly versions: Map<string, VersionSchema>;
    latestVersion: string;
    mode: EvolutionMode;
    constructor(name: string);
    /**
     * Set evolution mode.
     */
    withMode(mode: EvolutionMode): this;
    /**
     * Add a version with fields.
     */
    addVersion(version: string, fields: Record<string, EvolvingFieldConfig>): void;
    /**
     * Get schema for a specific version.
     */
    getVersion(version: string): VersionSchema | undefined;
    /**
     * Parse data from a specific version.
     */
    parse(data: Record<string, FieldValue>, fromVersion: string): ParseResult;
    /**
     * Emit version header for data.
     */
    emit(data: Record<string, FieldValue>, version?: string): EmitResult;
    /**
     * Migrate data between versions.
     */
    private migrate;
    /**
     * Migrate one step.
     */
    private migrateStep;
    /**
     * Get migration path between versions.
     */
    private getMigrationPath;
    /**
     * Get the latest version string.
     */
    private getLatestVersion;
    /**
     * Get changelog of schema evolution.
     */
    getChangelog(): ChangelogEntry[];
}
/**
 * Compare two version strings.
 * Returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
 */
export declare function compareVersions(v1: string, v2: string): number;
/**
 * Parse a version header (e.g., "@version 2.0").
 */
export declare function parseVersionHeader(text: string): string | null;
/**
 * Format a version header.
 */
export declare function formatVersionHeader(version: string): string;
/**
 * Create a versioned schema.
 */
export declare function versionedSchema(name: string): VersionedSchema;
//# sourceMappingURL=schema_evolution.d.ts.map