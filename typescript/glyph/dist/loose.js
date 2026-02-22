"use strict";
/**
 * GLYPH-Loose Mode
 *
 * Schema-optional canonicalization for GLYPH values.
 * Provides deterministic string representation for hashing, comparison, and deduplication.
 *
 * Canonical rules:
 * - null → "∅"
 * - bool → "t" / "f"
 * - int → decimal, no leading zeros, -0 → 0
 * - float → shortest roundtrip, E→e, -0→0
 * - string → bare if safe, otherwise quoted
 * - bytes → "b64" + quoted base64
 * - time → ISO-8601 UTC
 * - id → ^prefix:value or ^"quoted"
 * - list → "[" + space-separated elements + "]"
 * - map → "{" + sorted key=value pairs + "}"
 *   Keys sorted by bytewise UTF-8 of canonString(key)
 *
 * Auto-Tabular (v2.3.0):
 * - Homogeneous lists of objects can be emitted as @tab _ [cols]...|row|...@end
 * - Opt-in via LooseCanonOpts.autoTabular
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaultLooseCanonOpts = defaultLooseCanonOpts;
exports.llmLooseCanonOpts = llmLooseCanonOpts;
exports.noTabularLooseCanonOpts = noTabularLooseCanonOpts;
exports.prettyLooseCanonOpts = prettyLooseCanonOpts;
exports.tabularLooseCanonOpts = tabularLooseCanonOpts;
exports.canonicalizeLoose = canonicalizeLoose;
exports.canonicalizeLooseNoTabular = canonicalizeLooseNoTabular;
exports.canonicalizeLooseWithOpts = canonicalizeLooseWithOpts;
exports.canonicalizeLooseTabular = canonicalizeLooseTabular;
exports.unescapeTabularCell = unescapeTabularCell;
exports.parseTabularLoose = parseTabularLoose;
exports.parseTabularLooseHeaderWithMeta = parseTabularLooseHeaderWithMeta;
exports.fingerprintLoose = fingerprintLoose;
exports.equalLoose = equalLoose;
exports.canonicalizeLooseWithSchema = canonicalizeLooseWithSchema;
exports.buildKeyDictFromValue = buildKeyDictFromValue;
exports.parseSchemaHeader = parseSchemaHeader;
exports.fromJsonLoose = fromJsonLoose;
exports.toJsonLoose = toJsonLoose;
exports.parseJsonLoose = parseJsonLoose;
exports.stringifyJsonLoose = stringifyJsonLoose;
exports.jsonEqual = jsonEqual;
const types_1 = require("./types");
/**
 * Default options for loose canonicalization with smart auto-tabular ENABLED.
 * Lists of 3+ homogeneous objects are automatically emitted as @tab blocks.
 * Non-eligible data gracefully falls back to standard format.
 * Uses _ for null (ASCII-safe, LLM-friendly - matches Go/Python).
 */
function defaultLooseCanonOpts() {
    return {
        autoTabular: true,
        minRows: 3,
        maxCols: 20,
        allowMissing: true,
        nullStyle: 'underscore',
    };
}
/**
 * Options optimized for LLM output.
 * Uses _ for null (ASCII-safe, single token), auto-tabular enabled.
 */
function llmLooseCanonOpts() {
    return {
        autoTabular: true,
        minRows: 3,
        maxCols: 20,
        allowMissing: true,
        nullStyle: 'underscore',
    };
}
/**
 * Options with auto-tabular DISABLED.
 * Use for backward compatibility or when tabular format is not desired.
 */
function noTabularLooseCanonOpts() {
    return {
        autoTabular: false,
        minRows: 3,
        maxCols: 20,
        allowMissing: true,
        nullStyle: 'symbol',
    };
}
/**
 * Options for human-readable "pretty" output.
 * Uses ∅ for null (unicode symbol) for nicer visual appearance.
 */
function prettyLooseCanonOpts() {
    return {
        autoTabular: true,
        minRows: 3,
        maxCols: 20,
        allowMissing: true,
        nullStyle: 'symbol',
    };
}
/**
 * Options preset for tabular-enabled canonicalization.
 * @deprecated auto-tabular is now the default.
 */
function tabularLooseCanonOpts() {
    return defaultLooseCanonOpts();
}
// ============================================================
// Canonical Scalar Encoding
// ============================================================
const NULL_SYMBOL = '∅';
const NULL_UNDERSCORE = '_';
function canonNull() {
    return NULL_SYMBOL;
}
function canonNullWithStyle(style) {
    if (style === 'underscore') {
        return NULL_UNDERSCORE;
    }
    return NULL_SYMBOL;
}
function canonBool(v) {
    return v ? 't' : 'f';
}
function canonInt(n) {
    if (n === 0)
        return '0';
    return String(Math.floor(n));
}
function canonFloat(f) {
    if (f === 0)
        return '0';
    if (Object.is(f, -0))
        return '0'; // Negative zero -> 0
    // Use Go-compatible formatting (%g format)
    // Go's %g uses exponential for values with exponent < -4 or >= precision (default 6)
    const absF = Math.abs(f);
    // Match Go's strconv.FormatFloat with 'g' and -1 precision
    // Go switches to exponential when exponent is outside [-4, precision-1]
    // For -1 precision, it uses the minimum precision needed
    let s;
    // Calculate the exponent of the number
    const exp = absF === 0 ? 0 : Math.floor(Math.log10(absF));
    // Go uses exponential notation when exponent < -4 or when it saves space
    if (absF !== 0 && (exp < -4 || exp >= 15)) {
        // Use exponential notation
        s = f.toExponential();
        // Remove unnecessary trailing zeros in the mantissa
        s = s.replace(/\.?0+e/, 'e');
        // Pad the exponent to 2 digits to match Go
        s = s.replace(/e([+-])(\d)$/, 'e$10$2');
    }
    else {
        // Use regular notation
        s = String(f);
    }
    // Normalize: E -> e
    s = s.replace('E', 'e');
    return s;
}
function canonString(s) {
    if (isBareSafe(s)) {
        return s;
    }
    return quoteString(s);
}
function canonRef(prefix, value) {
    const full = prefix ? `${prefix}:${value}` : value;
    if (isRefSafe(full)) {
        return `^${full}`;
    }
    return `^${quoteString(full)}`;
}
function canonTime(d) {
    // ISO-8601 UTC format, trimmed to seconds
    return d.toISOString().replace(/\.\d{3}Z$/, 'Z');
}
function canonBytes(bytes) {
    if (bytes.length === 0) {
        return 'b64""';
    }
    return 'b64' + quoteString(bytesToBase64(bytes));
}
// ============================================================
// Safety Checks
// ============================================================
function isBareSafe(s) {
    if (s.length === 0)
        return false;
    // Reserved words
    if (['t', 'f', 'true', 'false', 'null', 'none', 'nil'].includes(s)) {
        return false;
    }
    // Use codepoint iteration for proper Unicode handling
    const codepoints = [...s].map(c => c.codePointAt(0));
    // First char: letter or underscore
    const first = codepoints[0];
    if (!isLetterCodepoint(first) && first !== 95)
        return false; // 95 = '_'
    // Rest: letter, digit, _, -, ., /
    for (let i = 1; i < codepoints.length; i++) {
        const c = codepoints[i];
        if (!isLetterCodepoint(c) && !isDigitCodepoint(c) && c !== 95 && c !== 45 && c !== 46 && c !== 47) {
            return false;
        }
    }
    return true;
}
function isLetterCodepoint(c) {
    // ASCII letters
    if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122)) {
        return true;
    }
    // Unicode letters - match Go's unicode.IsLetter behavior
    return c > 127 && /\p{L}/u.test(String.fromCodePoint(c));
}
function isDigitCodepoint(c) {
    // ASCII digits only for base case
    if (c >= 48 && c <= 57)
        return true;
    // Unicode digits - match Go's unicode.IsDigit behavior
    return c > 127 && /\p{Nd}/u.test(String.fromCodePoint(c));
}
function isRefSafe(s) {
    if (s.length === 0)
        return false;
    const codepoints = [...s].map(c => c.codePointAt(0));
    for (const c of codepoints) {
        if (!isLetterCodepoint(c) && !isDigitCodepoint(c) && c !== 95 && c !== 45 && c !== 46 && c !== 47 && c !== 58) {
            return false; // 58 = ':'
        }
    }
    return true;
}
// ============================================================
// String Quoting
// ============================================================
function quoteString(s) {
    let result = '"';
    for (const ch of s) {
        switch (ch) {
            case '\\':
                result += '\\\\';
                break;
            case '"':
                result += '\\"';
                break;
            case '\n':
                result += '\\n';
                break;
            case '\r':
                result += '\\r';
                break;
            case '\t':
                result += '\\t';
                break;
            default:
                const code = ch.charCodeAt(0);
                if (code < 0x20) {
                    result += '\\u' + code.toString(16).padStart(4, '0').toUpperCase();
                }
                else {
                    result += ch;
                }
        }
    }
    return result + '"';
}
// ============================================================
// Base64 Encoding
// ============================================================
function bytesToBase64(bytes) {
    if (typeof btoa === 'function') {
        let binary = '';
        for (let i = 0; i < bytes.length; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    }
    return Buffer.from(bytes).toString('base64');
}
// ============================================================
// Canonicalization
// ============================================================
/**
 * Returns a deterministic canonical string for any GValue.
 * This function produces identical output for semantically equal values,
 * making it suitable for hashing, comparison, and deduplication.
 *
 * Smart auto-tabular is ENABLED by default (v2.3.0+):
 * - Lists of 3+ homogeneous objects → @tab blocks (35-65% token savings)
 * - All other data → standard format (unchanged)
 *
 * Use canonicalizeLooseNoTabular for backward-compatible output.
 */
function canonicalizeLoose(v) {
    return canonicalizeLooseImpl(v, defaultLooseCanonOpts());
}
/**
 * Returns canonical form WITHOUT auto-tabular.
 * Use for v2.2.x backward compatibility or when tabular format is not desired.
 */
function canonicalizeLooseNoTabular(v) {
    return canonicalizeLooseWithOpts(v, noTabularLooseCanonOpts());
}
/**
 * Canonicalize with options (including auto-tabular support).
 */
function canonicalizeLooseWithOpts(v, opts) {
    return canonicalizeLooseImpl(v, { ...defaultLooseCanonOpts(), ...opts });
}
/**
 * Convenience function: canonicalize with auto-tabular enabled.
 * @deprecated auto-tabular is now the default. Use canonicalizeLoose instead.
 */
function canonicalizeLooseTabular(v) {
    return canonicalizeLoose(v);
}
function canonicalizeLooseImpl(v, opts) {
    switch (v.type) {
        case 'null':
            return canonNullWithStyle(opts.nullStyle);
        case 'bool':
            return canonBool(v.asBool());
        case 'int':
            return canonInt(v.asInt());
        case 'float':
            return canonFloat(v.asFloat());
        case 'str':
            return canonString(v.asStr());
        case 'bytes':
            return canonBytes(v.asBytes());
        case 'time':
            return canonTime(v.asTime());
        case 'id': {
            const ref = v.asId();
            return canonRef(ref.prefix, ref.value);
        }
        case 'list':
            return canonListLooseWithOpts(v.asList(), opts);
        case 'map':
            return canonMapLooseWithOpts(v.asMap(), opts);
        case 'struct':
            // Treat struct as map for loose canonicalization
            return canonMapLooseWithOpts(v.asStruct().fields, opts);
        case 'sum': {
            // Treat sum as {tag: value}
            const sum = v.asSum();
            const entry = { key: sum.tag, value: sum.value ?? types_1.GValue.null() };
            return canonMapLooseWithOpts([entry], opts);
        }
    }
}
/**
 * Canonical list form: "[" + space-separated + "]"
 * With opts, may emit @tab _ block for homogeneous lists.
 */
function canonListLooseWithOpts(items, opts) {
    if (items.length === 0) {
        return '[]';
    }
    // Check for tabular eligibility
    if (opts.autoTabular) {
        const cols = detectTabular(items, opts);
        if (cols !== null) {
            return emitTabularLoose(items, cols, opts);
        }
    }
    const parts = items.map(v => canonicalizeLooseImpl(v, opts));
    return '[' + parts.join(' ') + ']';
}
/**
 * Detect if a list of GValues qualifies for tabular emission.
 * Returns sorted column names if eligible, null otherwise.
 */
function detectTabular(items, opts) {
    const minRows = opts.minRows ?? 3;
    const maxCols = opts.maxCols ?? 20;
    const allowMissing = opts.allowMissing ?? true;
    if (items.length < minRows) {
        return null;
    }
    // Collect all keys from all rows
    const allKeys = new Set();
    const rowKeys = [];
    for (const item of items) {
        const entries = getMapEntries(item);
        if (entries === null) {
            return null; // Not a map/struct
        }
        const keys = new Set();
        for (const e of entries) {
            allKeys.add(e.key);
            keys.add(e.key);
        }
        rowKeys.push(keys);
    }
    if (allKeys.size === 0 || allKeys.size > maxCols) {
        return null;
    }
    // If not allowing missing keys, check all rows have same keys
    if (!allowMissing) {
        for (const keys of rowKeys) {
            if (keys.size !== allKeys.size) {
                return null;
            }
            for (const k of allKeys) {
                if (!keys.has(k)) {
                    return null;
                }
            }
        }
    }
    else {
        // Even with allowMissing, don't use tabular if items have mostly disjoint keys
        // (this would result in mostly-null rows which defeats the purpose)
        // Find common keys across all items
        let commonKeys = new Set(rowKeys[0]);
        for (let i = 1; i < rowKeys.length; i++) {
            const itemKeys = rowKeys[i];
            for (const k of commonKeys) {
                if (!itemKeys.has(k)) {
                    commonKeys.delete(k);
                }
            }
        }
        // If less than half the keys are common, don't use tabular
        if (commonKeys.size < allKeys.size / 2) {
            return null;
        }
    }
    // Sort keys by bytewise UTF-8 (same as canonString comparison)
    const cols = [...allKeys].sort((a, b) => {
        const ca = canonString(a);
        const cb = canonString(b);
        return ca < cb ? -1 : ca > cb ? 1 : 0;
    });
    return cols;
}
/**
 * Get map entries from a GValue (map or struct).
 * Returns null if not a map/struct.
 */
function getMapEntries(v) {
    if (v.type === 'map') {
        return v.asMap();
    }
    if (v.type === 'struct') {
        return v.asStruct().fields;
    }
    return null;
}
/**
 * Emit a list of maps/structs as @tab _ block.
 * v2.4.0: Includes rows/cols metadata for streaming resync.
 */
function emitTabularLoose(items, cols, opts) {
    const lines = [];
    // Header: @tab _ rows=N cols=M [col1 col2 col3]
    // The rows/cols metadata enables resync for streaming scenarios
    const headerCols = cols.map(c => {
        // Use compact keys if enabled
        if (opts.useCompactKeys && opts.keyDict) {
            const idx = opts.keyDict.indexOf(c);
            if (idx >= 0) {
                return `#${idx}`;
            }
        }
        return canonString(c);
    }).join(' ');
    lines.push(`@tab _ rows=${items.length} cols=${cols.length} [${headerCols}]`);
    // Rows: |val1|val2|val3|
    for (const item of items) {
        const entries = getMapEntries(item);
        const rowMap = new Map();
        for (const e of entries) {
            rowMap.set(e.key, e.value);
        }
        const cells = [];
        for (const col of cols) {
            const val = rowMap.get(col);
            if (val === undefined) {
                cells.push(canonNullWithStyle(opts.nullStyle));
            }
            else {
                cells.push(escapeTabularCell(canonicalizeLooseImpl(val, opts)));
            }
        }
        lines.push('|' + cells.join('|') + '|');
    }
    // Footer
    lines.push('@end');
    return lines.join('\n');
}
/**
 * Escape pipe characters in a tabular cell.
 * Only | needs escaping (as \|). Backslashes are NOT escaped - they're part of GLYPH string escapes.
 */
function escapeTabularCell(s) {
    return s.replace(/\|/g, '\\|');
}
/**
 * Unescape pipe characters in a tabular cell.
 */
function unescapeTabularCell(s) {
    return s.replace(/\\\|/g, '|');
}
/**
 * Parse a @tab _ block into a list of maps.
 * Input format:
 *   @tab _ [col1 col2 col3]
 *   |val1|val2|val3|
 *   |val4|val5|val6|
 *   @end
 */
function parseTabularLoose(input) {
    const lines = input.split('\n').map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length < 2) {
        throw new Error('tabular block requires at least header and @end');
    }
    // Parse header
    const header = lines[0];
    if (!header.startsWith('@tab _')) {
        throw new Error('expected @tab _ header');
    }
    const cols = parseTabularLooseHeader(header);
    if (cols.length === 0) {
        throw new Error('no columns found in header');
    }
    // Parse rows
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i];
        if (line === '@end') {
            break;
        }
        const row = parseTabularLooseRow(line, cols);
        rows.push(row);
    }
    return { columns: cols, rows };
}
/**
 * Parse the header line: @tab _ [col1 col2 col3]
 * Also accepts v2.4.0 format: @tab _ rows=N cols=M [col1 col2 col3]
 */
function parseTabularLooseHeader(line) {
    return parseTabularLooseHeaderWithMeta(line).keys;
}
/**
 * Parse header with full metadata.
 */
function parseTabularLooseHeaderWithMeta(line) {
    // Remove @tab _ prefix
    let rest = line.slice(line.indexOf('_') + 1).trim();
    const meta = { rows: -1, cols: -1, keys: [] };
    // Parse optional rows=N and cols=M before [
    while (!rest.startsWith('[') && rest.length > 0) {
        if (rest.startsWith('rows=')) {
            rest = rest.slice(5);
            const end = rest.search(/[\s\[]/);
            if (end === -1) {
                throw new Error('invalid rows= value');
            }
            meta.rows = parseInt(rest.slice(0, end), 10);
            rest = rest.slice(end).trim();
        }
        else if (rest.startsWith('cols=')) {
            rest = rest.slice(5);
            const end = rest.search(/[\s\[]/);
            if (end === -1) {
                throw new Error('invalid cols= value');
            }
            meta.cols = parseInt(rest.slice(0, end), 10);
            rest = rest.slice(end).trim();
        }
        else {
            // Skip unknown attributes
            const spaceIdx = rest.indexOf(' ');
            const bracketIdx = rest.indexOf('[');
            if (spaceIdx === -1 && bracketIdx === -1) {
                throw new Error(`expected '[' in header, got: ${rest}`);
            }
            if (spaceIdx >= 0 && (bracketIdx === -1 || spaceIdx < bracketIdx)) {
                rest = rest.slice(spaceIdx).trim();
            }
            else {
                break;
            }
        }
    }
    // Find the bracket content
    const start = rest.indexOf('[');
    const end = rest.lastIndexOf(']');
    if (start === -1 || end === -1 || end <= start) {
        throw new Error('malformed header: missing brackets');
    }
    const content = rest.slice(start + 1, end).trim();
    if (content.length === 0) {
        meta.keys = [];
    }
    else {
        // Split by spaces, handling quoted strings
        meta.keys = parseSpaceSeparatedValues(content);
    }
    return meta;
}
/**
 * Parse a row line: |val1|val2|val3|
 */
function parseTabularLooseRow(line, cols) {
    if (!line.startsWith('|') || !line.endsWith('|')) {
        throw new Error('row must start and end with |');
    }
    // Split by | respecting escapes
    const cells = splitTabularCells(line.slice(1, -1));
    const row = {};
    for (let i = 0; i < cols.length && i < cells.length; i++) {
        const cell = unescapeTabularCell(cells[i]);
        row[cols[i]] = parseLooseValue(cell);
    }
    return row;
}
/**
 * Split a row by | respecting \| escapes.
 */
function splitTabularCells(s) {
    const cells = [];
    let current = '';
    let i = 0;
    while (i < s.length) {
        if (s[i] === '\\' && i + 1 < s.length && s[i + 1] === '|') {
            current += '\\|';
            i += 2;
        }
        else if (s[i] === '|') {
            cells.push(current);
            current = '';
            i++;
        }
        else {
            current += s[i];
            i++;
        }
    }
    cells.push(current);
    return cells;
}
/**
 * Parse space-separated values, handling quoted strings.
 */
function parseSpaceSeparatedValues(s) {
    const values = [];
    let i = 0;
    while (i < s.length) {
        // Skip whitespace
        while (i < s.length && /\s/.test(s[i]))
            i++;
        if (i >= s.length)
            break;
        if (s[i] === '"') {
            // Quoted string
            const end = findClosingQuote(s, i);
            values.push(unquoteString(s.slice(i, end + 1)));
            i = end + 1;
        }
        else {
            // Bare value
            let end = i;
            while (end < s.length && !/\s/.test(s[end]))
                end++;
            values.push(s.slice(i, end));
            i = end;
        }
    }
    return values;
}
/**
 * Find closing quote, handling escapes.
 */
function findClosingQuote(s, start) {
    let i = start + 1;
    while (i < s.length) {
        if (s[i] === '\\' && i + 1 < s.length) {
            i += 2; // Skip escape
        }
        else if (s[i] === '"') {
            return i;
        }
        else {
            i++;
        }
    }
    throw new Error('unclosed quote');
}
/**
 * Parse a single loose value (cell content).
 */
function parseLooseValue(s) {
    s = s.trim();
    // Null - accept all aliases: ∅, _, null
    if (s === '∅' || s === '_' || s === 'null')
        return null;
    // Bool
    if (s === 't')
        return true;
    if (s === 'f')
        return false;
    // Quoted string
    if (s.startsWith('"') && s.endsWith('"')) {
        return unquoteString(s);
    }
    // Number (try to parse)
    const num = tryParseNumber(s);
    if (num !== null)
        return num;
    // Nested map
    if (s.startsWith('{') && s.endsWith('}')) {
        return parseLooseMap(s);
    }
    // Nested list
    if (s.startsWith('[') && s.endsWith(']')) {
        return parseLooseList(s);
    }
    // ID reference
    if (s.startsWith('^')) {
        return s; // Return as string for simplicity
    }
    // Bare string
    return s;
}
/**
 * Try to parse a number from string.
 */
function tryParseNumber(s) {
    if (!/^-?\d/.test(s) && s !== '-0')
        return null;
    const n = Number(s);
    if (Number.isNaN(n))
        return null;
    return n;
}
/**
 * Unquote a quoted string.
 */
function unquoteString(s) {
    if (!s.startsWith('"') || !s.endsWith('"')) {
        return s;
    }
    let result = '';
    let i = 1;
    while (i < s.length - 1) {
        if (s[i] === '\\' && i + 1 < s.length - 1) {
            const next = s[i + 1];
            switch (next) {
                case 'n':
                    result += '\n';
                    break;
                case 'r':
                    result += '\r';
                    break;
                case 't':
                    result += '\t';
                    break;
                case '"':
                    result += '"';
                    break;
                case '\\':
                    result += '\\';
                    break;
                case 'u':
                    if (i + 5 < s.length) {
                        const hex = s.slice(i + 2, i + 6);
                        result += String.fromCharCode(parseInt(hex, 16));
                        i += 4;
                    }
                    break;
                default:
                    result += next;
            }
            i += 2;
        }
        else {
            result += s[i];
            i++;
        }
    }
    return result;
}
/**
 * Parse a loose map: {key1=val1 key2=val2}
 */
function parseLooseMap(s) {
    const inner = s.slice(1, -1).trim();
    if (inner.length === 0)
        return {};
    const result = {};
    let i = 0;
    while (i < inner.length) {
        // Skip whitespace
        while (i < inner.length && /\s/.test(inner[i]))
            i++;
        if (i >= inner.length)
            break;
        // Parse key
        let key;
        if (inner[i] === '"') {
            const end = findClosingQuote(inner, i);
            key = unquoteString(inner.slice(i, end + 1));
            i = end + 1;
        }
        else {
            let end = i;
            while (end < inner.length && inner[end] !== '=' && !/\s/.test(inner[end]))
                end++;
            key = inner.slice(i, end);
            i = end;
        }
        // Skip = 
        while (i < inner.length && /\s/.test(inner[i]))
            i++;
        if (i >= inner.length || inner[i] !== '=') {
            throw new Error('expected = after key');
        }
        i++;
        // Skip whitespace after =
        while (i < inner.length && /\s/.test(inner[i]))
            i++;
        // Parse value
        const valueEnd = findValueEnd(inner, i);
        const valueStr = inner.slice(i, valueEnd);
        result[key] = parseLooseValue(valueStr);
        i = valueEnd;
    }
    return result;
}
/**
 * Parse a loose list: [val1 val2 val3]
 */
function parseLooseList(s) {
    const inner = s.slice(1, -1).trim();
    if (inner.length === 0)
        return [];
    const result = [];
    let i = 0;
    while (i < inner.length) {
        // Skip whitespace
        while (i < inner.length && /\s/.test(inner[i]))
            i++;
        if (i >= inner.length)
            break;
        const valueEnd = findValueEnd(inner, i);
        const valueStr = inner.slice(i, valueEnd);
        result.push(parseLooseValue(valueStr));
        i = valueEnd;
    }
    return result;
}
/**
 * Find the end of a value (respecting nesting and quotes).
 */
function findValueEnd(s, start) {
    let i = start;
    let depth = 0;
    let inQuote = false;
    while (i < s.length) {
        if (inQuote) {
            if (s[i] === '\\' && i + 1 < s.length) {
                i += 2;
            }
            else if (s[i] === '"') {
                inQuote = false;
                i++;
            }
            else {
                i++;
            }
        }
        else {
            if (s[i] === '"') {
                inQuote = true;
                i++;
            }
            else if (s[i] === '{' || s[i] === '[') {
                depth++;
                i++;
            }
            else if (s[i] === '}' || s[i] === ']') {
                depth--;
                i++;
            }
            else if (/\s/.test(s[i]) && depth === 0) {
                break;
            }
            else {
                i++;
            }
        }
    }
    return i;
}
/**
 * Canonical map form: "{" + sorted key=value pairs + "}"
 * Keys sorted by bytewise UTF-8 of canonString(key)
 */
function canonMapLoose(entries) {
    return canonMapLooseWithOpts(entries, defaultLooseCanonOpts());
}
/**
 * Canonical map form with options for compact keys.
 */
function canonMapLooseWithOpts(entries, opts) {
    if (entries.length === 0) {
        return '{}';
    }
    // Create sorted copy of entries
    const sorted = [...entries].sort((a, b) => {
        const ka = canonString(a.key);
        const kb = canonString(b.key);
        return ka < kb ? -1 : ka > kb ? 1 : 0;
    });
    const parts = sorted.map(e => {
        // Use compact key if enabled and key is in dictionary
        let keyStr;
        if (opts.useCompactKeys && opts.keyDict) {
            const idx = opts.keyDict.indexOf(e.key);
            if (idx >= 0) {
                keyStr = `#${idx}`;
            }
            else {
                keyStr = canonString(e.key);
            }
        }
        else {
            keyStr = canonString(e.key);
        }
        return `${keyStr}=${canonicalizeLooseImpl(e.value, opts)}`;
    });
    return '{' + parts.join(' ') + '}';
}
// ============================================================
// Loose Mode Helpers
// ============================================================
/**
 * Returns a deterministic fingerprint string for a GValue.
 * Useful for caching, deduplication, and equality checks.
 */
function fingerprintLoose(v) {
    return canonicalizeLoose(v);
}
/**
 * Checks if two GValues are semantically equal using loose canonicalization.
 */
function equalLoose(a, b) {
    return canonicalizeLoose(a) === canonicalizeLoose(b);
}
// ============================================================
// GLYPH v2.4.0: Schema Header + Compact Keys
// ============================================================
/**
 * Returns canonical form with schema header.
 * If opts.keyDict is set and opts.useCompactKeys is true, keys are emitted as #N.
 * If opts.schemaRef is set, a @schema header is prepended.
 */
function canonicalizeLooseWithSchema(v, opts) {
    const fullOpts = { ...defaultLooseCanonOpts(), ...opts };
    const parts = [];
    // Emit schema header if configured
    if (fullOpts.schemaRef || (fullOpts.keyDict && fullOpts.keyDict.length > 0)) {
        parts.push(emitSchemaHeader(fullOpts));
    }
    // Emit the value
    parts.push(canonicalizeLooseImpl(v, fullOpts));
    return parts.join('\n');
}
/**
 * Creates the @schema header line.
 * Format: @schema#<hash> keys=[key1 key2 ...]
 */
function emitSchemaHeader(opts) {
    const parts = ['@schema'];
    if (opts.schemaRef) {
        parts[0] += `#${opts.schemaRef}`;
    }
    if (opts.keyDict && opts.keyDict.length > 0) {
        const keys = opts.keyDict.map(k => canonString(k)).join(' ');
        parts.push(`keys=[${keys}]`);
    }
    return parts.join(' ');
}
/**
 * Extracts all unique keys from a value.
 * Useful for auto-generating a key dictionary for repeated objects.
 */
function buildKeyDictFromValue(v) {
    const keySet = new Set();
    collectKeys(v, keySet);
    return [...keySet].sort();
}
function collectKeys(v, keySet) {
    if (v.type === 'map') {
        for (const e of v.asMap()) {
            keySet.add(e.key);
            collectKeys(e.value, keySet);
        }
    }
    else if (v.type === 'struct') {
        for (const f of v.asStruct().fields) {
            keySet.add(f.key);
            collectKeys(f.value, keySet);
        }
    }
    else if (v.type === 'list') {
        for (const item of v.asList()) {
            collectKeys(item, keySet);
        }
    }
}
/**
 * Parses a @schema header line.
 * Returns schemaRef and keyDict, or throws on error.
 */
function parseSchemaHeader(line) {
    line = line.trim();
    if (!line.startsWith('@schema')) {
        throw new Error(`not a schema header: ${line}`);
    }
    let rest = line.slice('@schema'.length);
    let schemaRef = '';
    let keyDict = [];
    // Parse schema hash if present
    if (rest.startsWith('#')) {
        rest = rest.slice(1);
        const end = rest.indexOf(' ');
        if (end === -1) {
            schemaRef = rest;
            return { schemaRef, keyDict };
        }
        schemaRef = rest.slice(0, end);
        rest = rest.slice(end).trim();
    }
    // Parse keys= if present
    if (rest.startsWith('keys=')) {
        rest = rest.slice('keys='.length);
        if (!rest.startsWith('[')) {
            throw new Error(`keys= must be followed by []: ${rest}`);
        }
        const closeIdx = rest.indexOf(']');
        if (closeIdx === -1) {
            throw new Error(`missing ] in keys: ${rest}`);
        }
        const keysStr = rest.slice(1, closeIdx).trim();
        if (keysStr) {
            keyDict = keysStr.split(/\s+/);
        }
    }
    return { schemaRef, keyDict };
}
/**
 * Convert JSON value to GValue using loose mode.
 * Rejects NaN and Infinity for JSON compatibility.
 */
function fromJsonLoose(json, opts = {}) {
    if (json === null || json === undefined) {
        return types_1.GValue.null();
    }
    if (typeof json === 'boolean') {
        return types_1.GValue.bool(json);
    }
    if (typeof json === 'number') {
        // Reject NaN and Infinity in Loose mode
        if (!Number.isFinite(json)) {
            throw new Error('NaN/Infinity not allowed in GLYPH-Loose');
        }
        // Check if integer
        if (Number.isInteger(json) && Math.abs(json) <= Number.MAX_SAFE_INTEGER) {
            return types_1.GValue.int(json);
        }
        return types_1.GValue.float(json);
    }
    if (typeof json === 'string') {
        return types_1.GValue.str(json);
    }
    if (Array.isArray(json)) {
        const items = json.map(item => fromJsonLoose(item, opts));
        return types_1.GValue.list(...items);
    }
    if (typeof json === 'object') {
        const obj = json;
        // Check for extended markers
        if (opts.extended && typeof obj.$glyph === 'string') {
            return fromGlyphMarker(obj.$glyph, obj);
        }
        // Regular object/map
        const entries = [];
        for (const [key, val] of Object.entries(obj)) {
            entries.push({ key, value: fromJsonLoose(val, opts) });
        }
        return types_1.GValue.map(...entries);
    }
    throw new Error(`Unsupported JSON value type: ${typeof json}`);
}
function fromGlyphMarker(markerType, obj) {
    switch (markerType) {
        case 'time': {
            const value = obj.value;
            if (typeof value !== 'string') {
                throw new Error('$glyph time marker missing value');
            }
            return types_1.GValue.time(new Date(value));
        }
        case 'id': {
            const rawValue = obj.value;
            if (typeof rawValue !== 'string') {
                throw new Error('$glyph id marker missing value');
            }
            // Parse ^prefix:value format
            let value = rawValue;
            if (value.startsWith('^')) {
                value = value.slice(1);
            }
            const colonIdx = value.indexOf(':');
            if (colonIdx > 0) {
                return types_1.GValue.id(value.slice(0, colonIdx), value.slice(colonIdx + 1));
            }
            return types_1.GValue.id('', value);
        }
        case 'bytes': {
            const b64 = obj.base64;
            if (typeof b64 !== 'string') {
                throw new Error('$glyph bytes marker missing base64');
            }
            return types_1.GValue.bytes(base64ToBytes(b64));
        }
        default:
            throw new Error(`Unknown $glyph marker type: ${markerType}`);
    }
}
function base64ToBytes(b64) {
    if (typeof atob === 'function') {
        const binary = atob(b64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes;
    }
    return new Uint8Array(Buffer.from(b64, 'base64'));
}
/**
 * Convert GValue to JSON-compatible value using loose mode.
 * Rejects NaN and Infinity.
 */
function toJsonLoose(gv, opts = {}) {
    switch (gv.type) {
        case 'null':
            return null;
        case 'bool':
            return gv.asBool();
        case 'int':
            return gv.asInt();
        case 'float': {
            const f = gv.asFloat();
            if (!Number.isFinite(f)) {
                throw new Error('NaN/Infinity not allowed in JSON');
            }
            return f;
        }
        case 'str':
            return gv.asStr();
        case 'bytes': {
            const b64 = bytesToBase64(gv.asBytes());
            if (opts.extended) {
                return { $glyph: 'bytes', base64: b64 };
            }
            return b64;
        }
        case 'time': {
            const iso = gv.asTime().toISOString();
            if (opts.extended) {
                return { $glyph: 'time', value: iso };
            }
            return iso;
        }
        case 'id': {
            const ref = gv.asId();
            const refStr = `^${ref.prefix ? ref.prefix + ':' : ''}${ref.value}`;
            if (opts.extended) {
                return { $glyph: 'id', value: refStr };
            }
            return refStr;
        }
        case 'list':
            return gv.asList().map(v => toJsonLoose(v, opts));
        case 'map': {
            const result = {};
            for (const entry of gv.asMap()) {
                result[entry.key] = toJsonLoose(entry.value, opts);
            }
            return result;
        }
        case 'struct': {
            // Structs become objects
            const sv = gv.asStruct();
            const result = {};
            for (const field of sv.fields) {
                result[field.key] = toJsonLoose(field.value, opts);
            }
            return result;
        }
        case 'sum': {
            // Sums become { tag: value }
            const sum = gv.asSum();
            return { [sum.tag]: sum.value ? toJsonLoose(sum.value, opts) : null };
        }
    }
}
/**
 * Parse JSON string to GValue using loose mode.
 */
function parseJsonLoose(jsonStr, opts = {}) {
    const json = JSON.parse(jsonStr);
    return fromJsonLoose(json, opts);
}
/**
 * Stringify GValue to JSON string using loose mode.
 */
function stringifyJsonLoose(gv, opts = {}, indent) {
    const json = toJsonLoose(gv, opts);
    return JSON.stringify(json, null, indent);
}
/**
 * Check if two JSON byte arrays represent equal values.
 */
function jsonEqual(a, b) {
    const va = JSON.parse(a);
    const vb = JSON.parse(b);
    return jsonValueEqual(va, vb);
}
function jsonValueEqual(a, b) {
    if (a === b)
        return true;
    if (a === null || b === null)
        return a === b;
    if (typeof a !== typeof b)
        return false;
    if (Array.isArray(a)) {
        if (!Array.isArray(b) || a.length !== b.length)
            return false;
        for (let i = 0; i < a.length; i++) {
            if (!jsonValueEqual(a[i], b[i]))
                return false;
        }
        return true;
    }
    if (typeof a === 'object') {
        const objA = a;
        const objB = b;
        const keysA = Object.keys(objA);
        const keysB = Object.keys(objB);
        if (keysA.length !== keysB.length)
            return false;
        for (const key of keysA) {
            if (!(key in objB))
                return false;
            if (!jsonValueEqual(objA[key], objB[key]))
                return false;
        }
        return true;
    }
    return false;
}
//# sourceMappingURL=loose.js.map