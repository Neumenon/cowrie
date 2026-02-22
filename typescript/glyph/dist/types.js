"use strict";
/**
 * LYPH v2 Core Types
 *
 * GValue is the universal value type for LYPH/GLYPH data.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.g = exports.GValue = void 0;
exports.field = field;
/**
 * GValue - Universal value container for LYPH data
 */
class GValue {
    constructor(type) {
        this.type = type;
    }
    // ============================================================
    // Constructors
    // ============================================================
    static null() {
        return new GValue('null');
    }
    static bool(v) {
        const gv = new GValue('bool');
        gv._bool = v;
        return gv;
    }
    static int(v) {
        const gv = new GValue('int');
        gv._int = Math.floor(v);
        return gv;
    }
    static float(v) {
        const gv = new GValue('float');
        gv._float = v;
        return gv;
    }
    static str(v) {
        const gv = new GValue('str');
        gv._str = v;
        return gv;
    }
    static bytes(v) {
        const gv = new GValue('bytes');
        gv._bytes = v;
        return gv;
    }
    static time(v) {
        const gv = new GValue('time');
        gv._time = v;
        return gv;
    }
    static id(prefix, value) {
        const gv = new GValue('id');
        gv._id = { prefix, value };
        return gv;
    }
    static idFromRef(ref) {
        const gv = new GValue('id');
        gv._id = ref;
        return gv;
    }
    static list(...values) {
        const gv = new GValue('list');
        gv._list = values;
        return gv;
    }
    static map(...entries) {
        const gv = new GValue('map');
        gv._map = entries;
        return gv;
    }
    static struct(typeName, ...fields) {
        const gv = new GValue('struct');
        gv._struct = { typeName, fields };
        return gv;
    }
    static sum(tag, value) {
        const gv = new GValue('sum');
        gv._sum = { tag, value };
        return gv;
    }
    // ============================================================
    // Accessors
    // ============================================================
    isNull() {
        return this.type === 'null';
    }
    asBool() {
        if (this.type !== 'bool')
            throw new Error('not a bool');
        return this._bool;
    }
    asInt() {
        if (this.type !== 'int')
            throw new Error('not an int');
        return this._int;
    }
    asFloat() {
        if (this.type !== 'float')
            throw new Error('not a float');
        return this._float;
    }
    asStr() {
        if (this.type !== 'str')
            throw new Error('not a str');
        return this._str;
    }
    asBytes() {
        if (this.type !== 'bytes')
            throw new Error('not bytes');
        return this._bytes;
    }
    asTime() {
        if (this.type !== 'time')
            throw new Error('not a time');
        return this._time;
    }
    asId() {
        if (this.type !== 'id')
            throw new Error('not an id');
        return this._id;
    }
    asList() {
        if (this.type !== 'list')
            throw new Error('not a list');
        return this._list;
    }
    asMap() {
        if (this.type !== 'map')
            throw new Error('not a map');
        return this._map;
    }
    asStruct() {
        if (this.type !== 'struct')
            throw new Error('not a struct');
        return this._struct;
    }
    asSum() {
        if (this.type !== 'sum')
            throw new Error('not a sum');
        return this._sum;
    }
    /**
     * Get numeric value as number (works for int or float)
     */
    asNumber() {
        if (this.type === 'int')
            return this._int;
        if (this.type === 'float')
            return this._float;
        throw new Error('not a number');
    }
    /**
     * Get field from struct or map by key
     */
    get(key) {
        if (this.type === 'struct') {
            for (const f of this._struct.fields) {
                if (f.key === key)
                    return f.value;
            }
            return null;
        }
        if (this.type === 'map') {
            for (const e of this._map) {
                if (e.key === key)
                    return e.value;
            }
            return null;
        }
        return null;
    }
    /**
     * Get element from list by index
     */
    index(i) {
        if (this.type !== 'list')
            throw new Error('not a list');
        if (i < 0 || i >= this._list.length)
            throw new Error('index out of bounds');
        return this._list[i];
    }
    /**
     * Get length of list, map, or struct fields
     */
    len() {
        if (this.type === 'list')
            return this._list.length;
        if (this.type === 'map')
            return this._map.length;
        if (this.type === 'struct')
            return this._struct.fields.length;
        return 0;
    }
    // ============================================================
    // Mutators
    // ============================================================
    /**
     * Set field on struct or map
     */
    set(key, value) {
        if (this.type === 'struct') {
            for (let i = 0; i < this._struct.fields.length; i++) {
                if (this._struct.fields[i].key === key) {
                    this._struct.fields[i].value = value;
                    return;
                }
            }
            this._struct.fields.push({ key, value });
        }
        else if (this.type === 'map') {
            for (let i = 0; i < this._map.length; i++) {
                if (this._map[i].key === key) {
                    this._map[i].value = value;
                    return;
                }
            }
            this._map.push({ key, value });
        }
        else {
            throw new Error('cannot set on non-struct/map');
        }
    }
    /**
     * Append to list
     */
    append(value) {
        if (this.type !== 'list')
            throw new Error('cannot append to non-list');
        this._list.push(value);
    }
    // ============================================================
    // Deep Copy
    // ============================================================
    clone() {
        switch (this.type) {
            case 'null':
                return GValue.null();
            case 'bool':
                return GValue.bool(this._bool);
            case 'int':
                return GValue.int(this._int);
            case 'float':
                return GValue.float(this._float);
            case 'str':
                return GValue.str(this._str);
            case 'bytes':
                return GValue.bytes(new Uint8Array(this._bytes));
            case 'time':
                return GValue.time(new Date(this._time));
            case 'id':
                return GValue.id(this._id.prefix, this._id.value);
            case 'list':
                return GValue.list(...this._list.map(v => v.clone()));
            case 'map':
                return GValue.map(...this._map.map(e => ({ key: e.key, value: e.value.clone() })));
            case 'struct':
                return GValue.struct(this._struct.typeName, ...this._struct.fields.map(f => ({ key: f.key, value: f.value.clone() })));
            case 'sum':
                return GValue.sum(this._sum.tag, this._sum.value?.clone() ?? null);
        }
    }
}
exports.GValue = GValue;
// ============================================================
// Helper Functions
// ============================================================
/**
 * Create a field entry for struct construction
 */
function field(key, value) {
    return { key, value };
}
/**
 * Shorthand constructors
 */
exports.g = {
    null: GValue.null,
    bool: GValue.bool,
    int: GValue.int,
    float: GValue.float,
    str: GValue.str,
    bytes: GValue.bytes,
    time: GValue.time,
    id: GValue.id,
    list: GValue.list,
    map: GValue.map,
    struct: GValue.struct,
    sum: GValue.sum,
    field,
};
//# sourceMappingURL=types.js.map