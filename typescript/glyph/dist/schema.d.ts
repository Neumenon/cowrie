/**
 * LYPH v2 Schema System
 */
import { GValue } from './types';
export type TypeSpecKind = 'null' | 'bool' | 'int' | 'float' | 'str' | 'bytes' | 'time' | 'id' | 'list' | 'map' | 'ref' | 'inline';
export interface TypeSpec {
    kind: TypeSpecKind;
    name?: string;
    elem?: TypeSpec;
    keyType?: TypeSpec;
    valType?: TypeSpec;
}
export interface FieldDef {
    name: string;
    type: TypeSpec;
    fid: number;
    wireKey?: string;
    optional?: boolean;
    keepNull?: boolean;
    codec?: string;
    constraints?: Constraint[];
    defaultValue?: GValue;
}
export interface Constraint {
    kind: 'min' | 'max' | 'minLen' | 'maxLen' | 'len' | 'regex' | 'enum' | 'nonEmpty';
    value?: unknown;
}
export interface TypeDef {
    name: string;
    version?: string;
    kind: 'struct' | 'sum';
    fields?: FieldDef[];
    variants?: VariantDef[];
    packEnabled?: boolean;
    tabEnabled?: boolean;
    open?: boolean;
}
export interface VariantDef {
    tag: string;
    type: TypeSpec;
}
export declare class Schema {
    types: Map<string, TypeDef>;
    hash: string;
    getType(name: string): TypeDef | undefined;
    getField(typeName: string, fieldName: string): FieldDef | undefined;
    /**
     * Get fields sorted by FID
     */
    fieldsByFid(typeName: string): FieldDef[];
    /**
     * Get required fields sorted by FID
     */
    requiredFieldsByFid(typeName: string): FieldDef[];
    /**
     * Get optional fields sorted by FID
     */
    optionalFieldsByFid(typeName: string): FieldDef[];
    /**
     * Compute schema hash
     */
    computeHash(): string;
    /**
     * Get canonical representation
     */
    canonical(): string;
}
export declare class SchemaBuilder {
    private schema;
    private currentType?;
    /**
     * Add a struct type
     */
    addStruct(name: string, version?: string): SchemaBuilder;
    /**
     * Add a packed struct type (packed encoding enabled by default)
     */
    addPackedStruct(name: string, version?: string): SchemaBuilder;
    /**
     * Add an open struct type (accepts unknown fields)
     */
    addOpenStruct(name: string, version?: string): SchemaBuilder;
    /**
     * Add an open packed struct type (accepts unknown fields + packed encoding)
     */
    addOpenPackedStruct(name: string, version?: string): SchemaBuilder;
    /**
     * Add a field to the current struct
     */
    field(name: string, type: TypeSpec, options?: Partial<FieldDef>): SchemaBuilder;
    /**
     * Add a sum type
     */
    addSum(name: string, version?: string): SchemaBuilder;
    /**
     * Add a variant to the current sum type
     */
    variant(tag: string, type: TypeSpec): SchemaBuilder;
    /**
     * Enable packed encoding for a type
     */
    withPack(typeName: string): SchemaBuilder;
    /**
     * Enable tabular encoding for a type
     */
    withTab(typeName: string): SchemaBuilder;
    /**
     * Mark a type as open (accepts unknown fields)
     */
    withOpen(typeName: string): SchemaBuilder;
    /**
     * Build and return the schema
     */
    build(): Schema;
}
export declare const t: {
    null: () => TypeSpec;
    bool: () => TypeSpec;
    int: () => TypeSpec;
    float: () => TypeSpec;
    str: () => TypeSpec;
    bytes: () => TypeSpec;
    time: () => TypeSpec;
    id: () => TypeSpec;
    list: (elem: TypeSpec) => TypeSpec;
    map: (keyType: TypeSpec, valType: TypeSpec) => TypeSpec;
    ref: (name: string) => TypeSpec;
};
//# sourceMappingURL=schema.d.ts.map