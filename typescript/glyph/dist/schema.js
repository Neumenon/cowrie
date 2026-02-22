"use strict";
/**
 * LYPH v2 Schema System
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.t = exports.SchemaBuilder = exports.Schema = void 0;
// ============================================================
// Schema
// ============================================================
class Schema {
    constructor() {
        this.types = new Map();
        this.hash = '';
    }
    getType(name) {
        return this.types.get(name);
    }
    getField(typeName, fieldName) {
        const td = this.types.get(typeName);
        if (!td || td.kind !== 'struct' || !td.fields)
            return undefined;
        return td.fields.find(f => f.name === fieldName || f.wireKey === fieldName);
    }
    /**
     * Get fields sorted by FID
     */
    fieldsByFid(typeName) {
        const td = this.types.get(typeName);
        if (!td || !td.fields)
            return [];
        return [...td.fields].sort((a, b) => a.fid - b.fid);
    }
    /**
     * Get required fields sorted by FID
     */
    requiredFieldsByFid(typeName) {
        return this.fieldsByFid(typeName).filter(f => !f.optional);
    }
    /**
     * Get optional fields sorted by FID
     */
    optionalFieldsByFid(typeName) {
        return this.fieldsByFid(typeName).filter(f => f.optional);
    }
    /**
     * Compute schema hash
     */
    computeHash() {
        const canonical = this.canonical();
        // Simple hash for browser compatibility
        let hash = 0;
        for (let i = 0; i < canonical.length; i++) {
            const char = canonical.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        this.hash = Math.abs(hash).toString(16).padStart(8, '0');
        return this.hash;
    }
    /**
     * Get canonical representation
     */
    canonical() {
        const lines = ['@schema{'];
        const names = [...this.types.keys()].sort();
        for (const name of names) {
            const td = this.types.get(name);
            const openPrefix = td.open ? '@open ' : '';
            lines.push(`  ${name}${td.version ? ':' + td.version : ''} ${openPrefix}${td.kind}{`);
            if (td.kind === 'struct' && td.fields) {
                for (const f of td.fields) {
                    let line = `    ${f.name}: ${typeSpecToString(f.type)}`;
                    if (f.wireKey)
                        line += ` @k(${f.wireKey})`;
                    if (f.optional)
                        line += ' [optional]';
                    lines.push(line);
                }
            }
            lines.push('  }');
        }
        lines.push('}');
        return lines.join('\n');
    }
}
exports.Schema = Schema;
function typeSpecToString(ts) {
    switch (ts.kind) {
        case 'list':
            return `list<${typeSpecToString(ts.elem)}>`;
        case 'map':
            return `map<${typeSpecToString(ts.keyType)},${typeSpecToString(ts.valType)}>`;
        case 'ref':
            return ts.name;
        default:
            return ts.kind;
    }
}
// ============================================================
// Schema Builder
// ============================================================
class SchemaBuilder {
    constructor() {
        this.schema = new Schema();
    }
    /**
     * Add a struct type
     */
    addStruct(name, version) {
        this.currentType = {
            name,
            version,
            kind: 'struct',
            fields: [],
            tabEnabled: true,
        };
        this.schema.types.set(name, this.currentType);
        return this;
    }
    /**
     * Add a packed struct type (packed encoding enabled by default)
     */
    addPackedStruct(name, version) {
        this.currentType = {
            name,
            version,
            kind: 'struct',
            fields: [],
            packEnabled: true,
            tabEnabled: true,
        };
        this.schema.types.set(name, this.currentType);
        return this;
    }
    /**
     * Add an open struct type (accepts unknown fields)
     */
    addOpenStruct(name, version) {
        this.currentType = {
            name,
            version,
            kind: 'struct',
            fields: [],
            open: true,
            tabEnabled: true,
        };
        this.schema.types.set(name, this.currentType);
        return this;
    }
    /**
     * Add an open packed struct type (accepts unknown fields + packed encoding)
     */
    addOpenPackedStruct(name, version) {
        this.currentType = {
            name,
            version,
            kind: 'struct',
            fields: [],
            open: true,
            packEnabled: true,
            tabEnabled: true,
        };
        this.schema.types.set(name, this.currentType);
        return this;
    }
    /**
     * Add a field to the current struct
     */
    field(name, type, options) {
        if (!this.currentType || this.currentType.kind !== 'struct') {
            throw new Error('No struct type in progress');
        }
        const fid = options?.fid ?? this.currentType.fields.length + 1;
        this.currentType.fields.push({
            name,
            type,
            fid,
            ...options,
        });
        return this;
    }
    /**
     * Add a sum type
     */
    addSum(name, version) {
        this.currentType = {
            name,
            version,
            kind: 'sum',
            variants: [],
        };
        this.schema.types.set(name, this.currentType);
        return this;
    }
    /**
     * Add a variant to the current sum type
     */
    variant(tag, type) {
        if (!this.currentType || this.currentType.kind !== 'sum') {
            throw new Error('No sum type in progress');
        }
        this.currentType.variants.push({ tag, type });
        return this;
    }
    /**
     * Enable packed encoding for a type
     */
    withPack(typeName) {
        const td = this.schema.types.get(typeName);
        if (td)
            td.packEnabled = true;
        return this;
    }
    /**
     * Enable tabular encoding for a type
     */
    withTab(typeName) {
        const td = this.schema.types.get(typeName);
        if (td)
            td.tabEnabled = true;
        return this;
    }
    /**
     * Mark a type as open (accepts unknown fields)
     */
    withOpen(typeName) {
        const td = this.schema.types.get(typeName);
        if (td)
            td.open = true;
        return this;
    }
    /**
     * Build and return the schema
     */
    build() {
        this.schema.computeHash();
        return this.schema;
    }
}
exports.SchemaBuilder = SchemaBuilder;
// ============================================================
// Type Spec Helpers
// ============================================================
exports.t = {
    null: () => ({ kind: 'null' }),
    bool: () => ({ kind: 'bool' }),
    int: () => ({ kind: 'int' }),
    float: () => ({ kind: 'float' }),
    str: () => ({ kind: 'str' }),
    bytes: () => ({ kind: 'bytes' }),
    time: () => ({ kind: 'time' }),
    id: () => ({ kind: 'id' }),
    list: (elem) => ({ kind: 'list', elem }),
    map: (keyType, valType) => ({ kind: 'map', keyType, valType }),
    ref: (name) => ({ kind: 'ref', name }),
};
//# sourceMappingURL=schema.js.map