"use strict";
/**
 * GLYPH Streaming Validator
 *
 * Validates GLYPH tool calls incrementally as tokens arrive from an LLM.
 *
 * This enables:
 * - Early tool detection: Know the tool name before full response
 * - Early rejection: Stop on unknown tools mid-stream
 * - Incremental validation: Check constraints as tokens arrive
 * - Latency savings: Reject bad payloads without waiting for completion
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamingValidator = exports.ValidatorState = exports.ErrorCode = exports.ToolRegistry = void 0;
exports.defaultToolRegistry = defaultToolRegistry;
class ToolRegistry {
    constructor() {
        this.tools = new Map();
    }
    /**
     * Register a tool.
     */
    register(tool) {
        this.tools.set(tool.name, tool);
    }
    /**
     * Check if a tool is allowed.
     */
    isAllowed(name) {
        return this.tools.has(name);
    }
    /**
     * Get a tool schema.
     */
    get(name) {
        return this.tools.get(name);
    }
}
exports.ToolRegistry = ToolRegistry;
// ============================================================
// Validation Errors
// ============================================================
var ErrorCode;
(function (ErrorCode) {
    ErrorCode["UnknownTool"] = "UNKNOWN_TOOL";
    ErrorCode["MissingRequired"] = "MISSING_REQUIRED";
    ErrorCode["MissingTool"] = "MISSING_TOOL";
    ErrorCode["ConstraintMin"] = "CONSTRAINT_MIN";
    ErrorCode["ConstraintMax"] = "CONSTRAINT_MAX";
    ErrorCode["ConstraintLen"] = "CONSTRAINT_LEN";
    ErrorCode["ConstraintPattern"] = "CONSTRAINT_PATTERN";
    ErrorCode["ConstraintEnum"] = "CONSTRAINT_ENUM";
    ErrorCode["InvalidType"] = "INVALID_TYPE";
})(ErrorCode || (exports.ErrorCode = ErrorCode = {}));
// ============================================================
// Validator State
// ============================================================
var ValidatorState;
(function (ValidatorState) {
    ValidatorState["Waiting"] = "waiting";
    ValidatorState["InObject"] = "in_object";
    ValidatorState["Complete"] = "complete";
    ValidatorState["Error"] = "error";
})(ValidatorState || (exports.ValidatorState = ValidatorState = {}));
class StreamingValidator {
    constructor(registry) {
        // Parser state
        this.buffer = '';
        this.state = ValidatorState.Waiting;
        this.depth = 0;
        this.inString = false;
        this.escapeNext = false;
        this.currentKey = '';
        this.currentVal = '';
        this.hasKey = false;
        // Parsed data
        this.toolName = null;
        this.fields = {};
        this.errors = [];
        // Timing
        this.tokenCount = 0;
        this.charCount = 0;
        this.startTime = 0;
        this.toolDetectedAtToken = 0;
        this.toolDetectedAtTime = 0;
        this.firstErrorAtToken = 0;
        this.firstErrorAtTime = 0;
        this.completeAtToken = 0;
        this.completeAtTime = 0;
        // Timeline
        this.timeline = [];
        this.registry = registry;
    }
    /**
     * Reset the validator for reuse.
     */
    reset() {
        this.buffer = '';
        this.state = ValidatorState.Waiting;
        this.depth = 0;
        this.inString = false;
        this.escapeNext = false;
        this.currentKey = '';
        this.currentVal = '';
        this.hasKey = false;
        this.toolName = null;
        this.fields = {};
        this.errors = [];
        this.tokenCount = 0;
        this.charCount = 0;
        this.startTime = 0;
        this.toolDetectedAtToken = 0;
        this.toolDetectedAtTime = 0;
        this.firstErrorAtToken = 0;
        this.firstErrorAtTime = 0;
        this.completeAtToken = 0;
        this.completeAtTime = 0;
        this.timeline = [];
    }
    /**
     * Start timing.
     */
    start() {
        this.startTime = Date.now();
    }
    /**
     * Process a token from the LLM.
     */
    pushToken(token) {
        if (this.startTime === 0) {
            this.start();
        }
        this.tokenCount++;
        for (const c of token) {
            this.charCount++;
            this.processChar(c);
        }
        const elapsed = Date.now() - this.startTime;
        // Record tool detection
        if (this.toolName && this.toolDetectedAtToken === 0) {
            this.toolDetectedAtToken = this.tokenCount;
            this.toolDetectedAtTime = elapsed;
            const allowed = this.registry.isAllowed(this.toolName);
            this.timeline.push({
                event: 'TOOL_DETECTED',
                token: this.tokenCount,
                charPos: this.charCount,
                elapsed,
                detail: `tool=${this.toolName} allowed=${allowed}`,
            });
        }
        // Record first error
        if (this.errors.length > 0 && this.firstErrorAtToken === 0) {
            this.firstErrorAtToken = this.tokenCount;
            this.firstErrorAtTime = elapsed;
            this.timeline.push({
                event: 'ERROR',
                token: this.tokenCount,
                charPos: this.charCount,
                elapsed,
                detail: this.errors[0].message,
            });
        }
        // Record completion
        if (this.state === ValidatorState.Complete && this.completeAtToken === 0) {
            this.completeAtToken = this.tokenCount;
            this.completeAtTime = elapsed;
            this.timeline.push({
                event: 'COMPLETE',
                token: this.tokenCount,
                charPos: this.charCount,
                elapsed,
                detail: `valid=${this.errors.length === 0}`,
            });
        }
        return this.getResult();
    }
    processChar(c) {
        this.buffer += c;
        // Handle escape sequences
        if (this.escapeNext) {
            this.escapeNext = false;
            this.currentVal += c;
            return;
        }
        if (c === '\\' && this.inString) {
            this.escapeNext = true;
            this.currentVal += c;
            return;
        }
        // Handle quotes
        if (c === '"') {
            if (this.inString) {
                this.inString = false;
            }
            else {
                this.inString = true;
                this.currentVal = '';
            }
            return;
        }
        // Inside string - accumulate
        if (this.inString) {
            this.currentVal += c;
            return;
        }
        // Handle structural characters
        switch (c) {
            case '{':
                if (this.state === ValidatorState.Waiting) {
                    this.state = ValidatorState.InObject;
                }
                this.depth++;
                break;
            case '}':
                this.depth--;
                if (this.depth === 0) {
                    this.finishField();
                    this.state = ValidatorState.Complete;
                    this.validateComplete();
                }
                break;
            case '[':
                this.depth++;
                this.currentVal += c;
                break;
            case ']':
                this.depth--;
                this.currentVal += c;
                break;
            case '=':
                if (this.depth === 1 && !this.hasKey) {
                    this.currentKey = this.currentVal.trim();
                    this.currentVal = '';
                    this.hasKey = true;
                }
                else {
                    this.currentVal += c;
                }
                break;
            case ' ':
            case '\n':
            case '\t':
            case '\r':
                if (this.depth === 1 && this.hasKey && this.currentVal.length > 0) {
                    this.finishField();
                }
                break;
            default:
                this.currentVal += c;
        }
    }
    finishField() {
        if (!this.hasKey) {
            return;
        }
        const key = this.currentKey;
        const valStr = this.currentVal.trim();
        this.currentKey = '';
        this.currentVal = '';
        this.hasKey = false;
        const value = this.parseValue(valStr);
        // Check for tool/action field
        if (key === 'action' || key === 'tool') {
            if (typeof value === 'string') {
                this.toolName = value;
                // Validate against allow list
                if (!this.registry.isAllowed(value)) {
                    this.errors.push({
                        code: ErrorCode.UnknownTool,
                        message: `Unknown tool: ${value}`,
                        field: key,
                    });
                }
            }
        }
        // Validate field constraints
        if (this.toolName) {
            this.validateField(key, value);
        }
        this.fields[key] = value;
    }
    parseValue(s) {
        // Boolean
        if (s === 't' || s === 'true') {
            return true;
        }
        if (s === 'f' || s === 'false') {
            return false;
        }
        // Null
        if (s === '_' || s === 'null' || s === '') {
            return null;
        }
        // Integer
        if (/^-?\d+$/.test(s)) {
            return parseInt(s, 10);
        }
        // Float
        if (/^-?\d+\.?\d*$/.test(s) || /^-?\d*\.?\d+$/.test(s)) {
            const f = parseFloat(s);
            if (!isNaN(f)) {
                return f;
            }
        }
        // String
        return s;
    }
    validateField(key, value) {
        if (key === 'action' || key === 'tool') {
            return;
        }
        const schema = this.registry.get(this.toolName);
        if (!schema) {
            return;
        }
        const argSchema = schema.args[key];
        if (!argSchema) {
            return;
        }
        // Numeric constraints
        if (typeof value === 'number') {
            if (argSchema.min !== undefined && value < argSchema.min) {
                this.errors.push({
                    code: ErrorCode.ConstraintMin,
                    message: `${key} < ${argSchema.min}`,
                    field: key,
                });
            }
            if (argSchema.max !== undefined && value > argSchema.max) {
                this.errors.push({
                    code: ErrorCode.ConstraintMax,
                    message: `${key} > ${argSchema.max}`,
                    field: key,
                });
            }
        }
        // String constraints
        if (typeof value === 'string') {
            if (argSchema.minLen !== undefined && value.length < argSchema.minLen) {
                this.errors.push({
                    code: ErrorCode.ConstraintLen,
                    message: `${key} length < ${argSchema.minLen}`,
                    field: key,
                });
            }
            if (argSchema.maxLen !== undefined && value.length > argSchema.maxLen) {
                this.errors.push({
                    code: ErrorCode.ConstraintLen,
                    message: `${key} length > ${argSchema.maxLen}`,
                    field: key,
                });
            }
            if (argSchema.pattern && !argSchema.pattern.test(value)) {
                this.errors.push({
                    code: ErrorCode.ConstraintPattern,
                    message: `${key} pattern mismatch`,
                    field: key,
                });
            }
            if (argSchema.enumValues && !argSchema.enumValues.includes(value)) {
                this.errors.push({
                    code: ErrorCode.ConstraintEnum,
                    message: `${key} not in allowed values`,
                    field: key,
                });
            }
        }
    }
    validateComplete() {
        if (!this.toolName) {
            this.errors.push({
                code: ErrorCode.MissingTool,
                message: 'No action field found',
            });
            return;
        }
        const schema = this.registry.get(this.toolName);
        if (!schema) {
            return;
        }
        // Check required fields
        for (const [argName, argSchema] of Object.entries(schema.args)) {
            if (argSchema.required && !(argName in this.fields)) {
                this.errors.push({
                    code: ErrorCode.MissingRequired,
                    message: `Missing required field: ${argName}`,
                    field: argName,
                });
            }
        }
    }
    /**
     * Get the current validation result.
     */
    getResult() {
        const toolAllowed = this.toolName ? this.registry.isAllowed(this.toolName) : null;
        return {
            complete: this.state === ValidatorState.Complete,
            valid: this.errors.length === 0,
            toolName: this.toolName,
            toolAllowed,
            errors: [...this.errors],
            fields: { ...this.fields },
            tokenCount: this.tokenCount,
            charCount: this.charCount,
            timeline: [...this.timeline],
            toolDetectedAtToken: this.toolDetectedAtToken,
            toolDetectedAtTime: this.toolDetectedAtTime,
            firstErrorAtToken: this.firstErrorAtToken,
            firstErrorAtTime: this.firstErrorAtTime,
            completeAtToken: this.completeAtToken,
            completeAtTime: this.completeAtTime,
        };
    }
    /**
     * Check if the stream should be cancelled.
     */
    shouldStop() {
        return this.errors.some(e => e.code === ErrorCode.UnknownTool);
    }
}
exports.StreamingValidator = StreamingValidator;
// ============================================================
// Default Registry
// ============================================================
/**
 * Create a default tool registry with common tools.
 */
function defaultToolRegistry() {
    const registry = new ToolRegistry();
    registry.register({
        name: 'search',
        description: 'Search for information',
        args: {
            query: { type: 'string', required: true, minLen: 1 },
            max_results: { type: 'int', min: 1, max: 100 },
        },
    });
    registry.register({
        name: 'calculate',
        description: 'Evaluate a mathematical expression',
        args: {
            expression: { type: 'string', required: true },
            precision: { type: 'int', min: 0, max: 15 },
        },
    });
    registry.register({
        name: 'browse',
        description: 'Fetch a web page',
        args: {
            url: { type: 'string', required: true, pattern: /^https?:\/\// },
        },
    });
    registry.register({
        name: 'execute',
        description: 'Execute a shell command',
        args: {
            command: { type: 'string', required: true },
        },
    });
    return registry;
}
//# sourceMappingURL=stream_validator.js.map