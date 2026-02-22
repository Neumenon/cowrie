/**
 * LYPH v2 Core Types
 *
 * GValue is the universal value type for LYPH/GLYPH data.
 */
export type GType = 'null' | 'bool' | 'int' | 'float' | 'str' | 'bytes' | 'time' | 'id' | 'list' | 'map' | 'struct' | 'sum';
export interface RefID {
    prefix: string;
    value: string;
}
export interface MapEntry {
    key: string;
    value: GValue;
}
export interface StructValue {
    typeName: string;
    fields: MapEntry[];
}
export interface SumValue {
    tag: string;
    value: GValue | null;
}
/**
 * GValue - Universal value container for LYPH data
 */
export declare class GValue {
    readonly type: GType;
    private _bool?;
    private _int?;
    private _float?;
    private _str?;
    private _bytes?;
    private _time?;
    private _id?;
    private _list?;
    private _map?;
    private _struct?;
    private _sum?;
    private constructor();
    static null(): GValue;
    static bool(v: boolean): GValue;
    static int(v: number): GValue;
    static float(v: number): GValue;
    static str(v: string): GValue;
    static bytes(v: Uint8Array): GValue;
    static time(v: Date): GValue;
    static id(prefix: string, value: string): GValue;
    static idFromRef(ref: RefID): GValue;
    static list(...values: GValue[]): GValue;
    static map(...entries: MapEntry[]): GValue;
    static struct(typeName: string, ...fields: MapEntry[]): GValue;
    static sum(tag: string, value: GValue | null): GValue;
    isNull(): boolean;
    asBool(): boolean;
    asInt(): number;
    asFloat(): number;
    asStr(): string;
    asBytes(): Uint8Array;
    asTime(): Date;
    asId(): RefID;
    asList(): GValue[];
    asMap(): MapEntry[];
    asStruct(): StructValue;
    asSum(): SumValue;
    /**
     * Get numeric value as number (works for int or float)
     */
    asNumber(): number;
    /**
     * Get field from struct or map by key
     */
    get(key: string): GValue | null;
    /**
     * Get element from list by index
     */
    index(i: number): GValue;
    /**
     * Get length of list, map, or struct fields
     */
    len(): number;
    /**
     * Set field on struct or map
     */
    set(key: string, value: GValue): void;
    /**
     * Append to list
     */
    append(value: GValue): void;
    clone(): GValue;
}
/**
 * Create a field entry for struct construction
 */
export declare function field(key: string, value: GValue): MapEntry;
/**
 * Shorthand constructors
 */
export declare const g: {
    null: typeof GValue.null;
    bool: typeof GValue.bool;
    int: typeof GValue.int;
    float: typeof GValue.float;
    str: typeof GValue.str;
    bytes: typeof GValue.bytes;
    time: typeof GValue.time;
    id: typeof GValue.id;
    list: typeof GValue.list;
    map: typeof GValue.map;
    struct: typeof GValue.struct;
    sum: typeof GValue.sum;
    field: typeof field;
};
//# sourceMappingURL=types.d.ts.map