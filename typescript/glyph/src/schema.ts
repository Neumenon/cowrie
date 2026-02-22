/**
 * LYPH v2 Schema System
 */

import { GValue } from './types';

// ============================================================
// Type Specifications
// ============================================================

export type TypeSpecKind = 
  | 'null' | 'bool' | 'int' | 'float' | 'str' | 'bytes' | 'time' | 'id'
  | 'list' | 'map' | 'ref' | 'inline';

export interface TypeSpec {
  kind: TypeSpecKind;
  name?: string;      // For ref types
  elem?: TypeSpec;    // For list types
  keyType?: TypeSpec; // For map types
  valType?: TypeSpec; // For map types
}

// ============================================================
// Field Definition
// ============================================================

export interface FieldDef {
  name: string;
  type: TypeSpec;
  fid: number;           // Stable field ID for packed encoding
  wireKey?: string;      // Short key for wire format
  optional?: boolean;
  keepNull?: boolean;    // Emit null even if optional
  codec?: string;        // Encoding hint
  constraints?: Constraint[];
  defaultValue?: GValue;
}

export interface Constraint {
  kind: 'min' | 'max' | 'minLen' | 'maxLen' | 'len' | 'regex' | 'enum' | 'nonEmpty';
  value?: unknown;
}

// ============================================================
// Type Definition
// ============================================================

export interface TypeDef {
  name: string;
  version?: string;
  kind: 'struct' | 'sum';
  fields?: FieldDef[];      // For struct
  variants?: VariantDef[];  // For sum
  packEnabled?: boolean;
  tabEnabled?: boolean;
  open?: boolean;           // @open: accept unknown fields
}

export interface VariantDef {
  tag: string;
  type: TypeSpec;
}

// ============================================================
// Schema
// ============================================================

export class Schema {
  types: Map<string, TypeDef> = new Map();
  hash: string = '';

  getType(name: string): TypeDef | undefined {
    return this.types.get(name);
  }

  getField(typeName: string, fieldName: string): FieldDef | undefined {
    const td = this.types.get(typeName);
    if (!td || td.kind !== 'struct' || !td.fields) return undefined;
    return td.fields.find(f => f.name === fieldName || f.wireKey === fieldName);
  }

  /**
   * Get fields sorted by FID
   */
  fieldsByFid(typeName: string): FieldDef[] {
    const td = this.types.get(typeName);
    if (!td || !td.fields) return [];
    return [...td.fields].sort((a, b) => a.fid - b.fid);
  }

  /**
   * Get required fields sorted by FID
   */
  requiredFieldsByFid(typeName: string): FieldDef[] {
    return this.fieldsByFid(typeName).filter(f => !f.optional);
  }

  /**
   * Get optional fields sorted by FID
   */
  optionalFieldsByFid(typeName: string): FieldDef[] {
    return this.fieldsByFid(typeName).filter(f => f.optional);
  }

  /**
   * Compute schema hash
   */
  computeHash(): string {
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
  canonical(): string {
    const lines: string[] = ['@schema{'];
    const names = [...this.types.keys()].sort();
    
    for (const name of names) {
      const td = this.types.get(name)!;
      const openPrefix = td.open ? '@open ' : '';
      lines.push(`  ${name}${td.version ? ':' + td.version : ''} ${openPrefix}${td.kind}{`);
      
      if (td.kind === 'struct' && td.fields) {
        for (const f of td.fields) {
          let line = `    ${f.name}: ${typeSpecToString(f.type)}`;
          if (f.wireKey) line += ` @k(${f.wireKey})`;
          if (f.optional) line += ' [optional]';
          lines.push(line);
        }
      }
      
      lines.push('  }');
    }
    
    lines.push('}');
    return lines.join('\n');
  }
}

function typeSpecToString(ts: TypeSpec): string {
  switch (ts.kind) {
    case 'list':
      return `list<${typeSpecToString(ts.elem!)}>`;
    case 'map':
      return `map<${typeSpecToString(ts.keyType!)},${typeSpecToString(ts.valType!)}>`;
    case 'ref':
      return ts.name!;
    default:
      return ts.kind;
  }
}

// ============================================================
// Schema Builder
// ============================================================

export class SchemaBuilder {
  private schema: Schema = new Schema();
  private currentType?: TypeDef;

  /**
   * Add a struct type
   */
  addStruct(name: string, version?: string): SchemaBuilder {
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
  addPackedStruct(name: string, version?: string): SchemaBuilder {
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
  addOpenStruct(name: string, version?: string): SchemaBuilder {
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
  addOpenPackedStruct(name: string, version?: string): SchemaBuilder {
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
  field(name: string, type: TypeSpec, options?: Partial<FieldDef>): SchemaBuilder {
    if (!this.currentType || this.currentType.kind !== 'struct') {
      throw new Error('No struct type in progress');
    }
    
    const fid = options?.fid ?? this.currentType.fields!.length + 1;
    
    this.currentType.fields!.push({
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
  addSum(name: string, version?: string): SchemaBuilder {
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
  variant(tag: string, type: TypeSpec): SchemaBuilder {
    if (!this.currentType || this.currentType.kind !== 'sum') {
      throw new Error('No sum type in progress');
    }
    this.currentType.variants!.push({ tag, type });
    return this;
  }

  /**
   * Enable packed encoding for a type
   */
  withPack(typeName: string): SchemaBuilder {
    const td = this.schema.types.get(typeName);
    if (td) td.packEnabled = true;
    return this;
  }

  /**
   * Enable tabular encoding for a type
   */
  withTab(typeName: string): SchemaBuilder {
    const td = this.schema.types.get(typeName);
    if (td) td.tabEnabled = true;
    return this;
  }

  /**
   * Mark a type as open (accepts unknown fields)
   */
  withOpen(typeName: string): SchemaBuilder {
    const td = this.schema.types.get(typeName);
    if (td) td.open = true;
    return this;
  }

  /**
   * Build and return the schema
   */
  build(): Schema {
    this.schema.computeHash();
    return this.schema;
  }
}

// ============================================================
// Type Spec Helpers
// ============================================================

export const t = {
  null: (): TypeSpec => ({ kind: 'null' }),
  bool: (): TypeSpec => ({ kind: 'bool' }),
  int: (): TypeSpec => ({ kind: 'int' }),
  float: (): TypeSpec => ({ kind: 'float' }),
  str: (): TypeSpec => ({ kind: 'str' }),
  bytes: (): TypeSpec => ({ kind: 'bytes' }),
  time: (): TypeSpec => ({ kind: 'time' }),
  id: (): TypeSpec => ({ kind: 'id' }),
  list: (elem: TypeSpec): TypeSpec => ({ kind: 'list', elem }),
  map: (keyType: TypeSpec, valType: TypeSpec): TypeSpec => ({ kind: 'map', keyType, valType }),
  ref: (name: string): TypeSpec => ({ kind: 'ref', name }),
};
