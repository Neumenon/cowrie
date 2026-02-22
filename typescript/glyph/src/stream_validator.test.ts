/**
 * GLYPH Streaming Validator Tests
 */

import {
  StreamingValidator,
  ToolRegistry,
  ToolSchema,
  ArgSchema,
  ValidationResult,
  ValidatorState,
  ErrorCode,
  ValidatorLimits,
  DEFAULT_MAX_BUFFER,
  DEFAULT_MAX_FIELDS,
  DEFAULT_MAX_ERRORS,
  defaultToolRegistry,
} from './stream_validator';

// ============================================================
// ToolRegistry Tests
// ============================================================

describe('ToolRegistry', () => {
  test('register and check tool', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test_tool',
      description: 'A test tool',
      args: {
        query: { type: 'string', required: true },
      },
    });

    expect(registry.isAllowed('test_tool')).toBe(true);
    expect(registry.isAllowed('unknown_tool')).toBe(false);
  });

  test('get tool schema', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'search',
      description: 'Search for information',
      args: {
        query: { type: 'string', required: true, minLen: 1 },
        max_results: { type: 'int', min: 1, max: 100 },
      },
    });

    const schema = registry.get('search');
    expect(schema).toBeDefined();
    expect(schema?.name).toBe('search');
    expect(schema?.args.query.required).toBe(true);
    expect(schema?.args.max_results.min).toBe(1);
  });

  test('get returns undefined for unknown tool', () => {
    const registry = new ToolRegistry();
    expect(registry.get('unknown')).toBeUndefined();
  });
});

// ============================================================
// Basic Parsing Tests
// ============================================================

describe('StreamingValidator - Basic Parsing', () => {
  let registry: ToolRegistry;

  beforeEach(() => {
    registry = new ToolRegistry();
    registry.register({
      name: 'search',
      args: { query: { type: 'string', required: true } },
    });
  });

  test('parse simple tool call', () => {
    const validator = new StreamingValidator(registry);

    // Simulate streaming tokens
    validator.pushToken('{');
    validator.pushToken('action=');
    validator.pushToken('search ');
    validator.pushToken('query=');
    validator.pushToken('"hello world"');
    const result = validator.pushToken('}');

    expect(result.complete).toBe(true);
    expect(result.valid).toBe(true);
    expect(result.toolName).toBe('search');
    expect(result.fields.query).toBe('hello world');
  });

  test('parse tool call with integer', () => {
    registry.register({
      name: 'count',
      args: { n: { type: 'int', required: true } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=count n=42}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
    expect(result.fields.n).toBe(42);
  });

  test('parse tool call with float', () => {
    registry.register({
      name: 'calc',
      args: { value: { type: 'float' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=calc value=3.14}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
    expect(result.fields.value).toBeCloseTo(3.14);
  });

  test('parse tool call with boolean true', () => {
    registry.register({
      name: 'toggle',
      args: { enabled: { type: 'bool' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=toggle enabled=t}');

    const result = validator.getResult();
    expect(result.fields.enabled).toBe(true);
  });

  test('parse tool call with boolean false', () => {
    registry.register({
      name: 'toggle',
      args: { enabled: { type: 'bool' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=toggle enabled=false}');

    const result = validator.getResult();
    expect(result.fields.enabled).toBe(false);
  });

  test('parse null values', () => {
    registry.register({
      name: 'test',
      args: { value: { type: 'string' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test value=_}');

    const result = validator.getResult();
    expect(result.fields.value).toBeNull();
  });

  test('parse quoted strings with spaces', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=search query="hello world test"}');

    const result = validator.getResult();
    expect(result.fields.query).toBe('hello world test');
  });

  test('parse escaped characters in strings', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=search query="hello\\"world"}');

    const result = validator.getResult();
    expect(result.fields.query).toBe('hello\\"world');
  });
});

// ============================================================
// Tool Detection Tests
// ============================================================

describe('StreamingValidator - Tool Detection', () => {
  let registry: ToolRegistry;

  beforeEach(() => {
    registry = new ToolRegistry();
    registry.register({
      name: 'search',
      args: { query: { type: 'string', required: true } },
    });
  });

  test('detect tool early in stream', () => {
    const validator = new StreamingValidator(registry);

    validator.pushToken('{');
    validator.pushToken('action=');
    const result = validator.pushToken('search ');

    expect(result.toolName).toBe('search');
    expect(result.toolAllowed).toBe(true);
    expect(result.complete).toBe(false);
  });

  test('detect unknown tool', () => {
    const validator = new StreamingValidator(registry);

    const result = validator.pushToken('{action=unknown_tool}');

    expect(result.toolName).toBe('unknown_tool');
    expect(result.toolAllowed).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0].code).toBe(ErrorCode.UnknownTool);
  });

  test('shouldStop returns true for unknown tool', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=unknown_tool}');

    expect(validator.shouldStop()).toBe(true);
  });

  test('shouldStop returns false for valid tool', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=search query="test"}');

    expect(validator.shouldStop()).toBe(false);
  });

  test('isToolAllowed helper method', () => {
    const validator = new StreamingValidator(registry);

    expect(validator.isToolAllowed()).toBe(false); // No tool detected yet

    validator.pushToken('{action=search}');
    expect(validator.isToolAllowed()).toBe(true);
  });
});

// ============================================================
// Constraint Validation Tests
// ============================================================

describe('StreamingValidator - Constraint Validation', () => {
  let registry: ToolRegistry;

  beforeEach(() => {
    registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: {
        count: { type: 'int', min: 1, max: 100 },
        name: { type: 'string', minLen: 2, maxLen: 10 },
        status: { type: 'string', enumValues: ['active', 'inactive'] },
        url: { type: 'string', pattern: /^https?:\/\// },
      },
    });
  });

  test('validate min constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test count=0}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintMin)).toBe(true);
  });

  test('validate max constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test count=101}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintMax)).toBe(true);
  });

  test('validate minLen constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test name=a}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintLen)).toBe(true);
  });

  test('validate maxLen constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test name="this is way too long"}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintLen)).toBe(true);
  });

  test('validate enum constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test status=pending}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintEnum)).toBe(true);
  });

  test('validate pattern constraint', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test url=not-a-url}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.ConstraintPattern)).toBe(true);
  });

  test('valid constraints pass', () => {
    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test count=50 name=valid status=active url="https://example.com"}');

    const result = validator.getResult();
    expect(result.valid).toBe(true);
    expect(result.errors.length).toBe(0);
  });
});

// ============================================================
// Required Fields Tests
// ============================================================

describe('StreamingValidator - Required Fields', () => {
  test('missing required field', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: {
        required_field: { type: 'string', required: true },
        optional_field: { type: 'string' },
      },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test optional_field=value}');

    const result = validator.getResult();
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.code === ErrorCode.MissingRequired)).toBe(true);
  });

  test('all required fields present', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: {
        field1: { type: 'string', required: true },
        field2: { type: 'int', required: true },
      },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test field1=hello field2=42}');

    const result = validator.getResult();
    expect(result.valid).toBe(true);
  });

  test('missing action field', () => {
    const registry = new ToolRegistry();
    const validator = new StreamingValidator(registry);
    validator.pushToken('{query=test}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.MissingTool)).toBe(true);
  });
});

// ============================================================
// Buffer Limits Tests
// ============================================================

describe('StreamingValidator - Buffer Limits', () => {
  test('default limits are set', () => {
    expect(DEFAULT_MAX_BUFFER).toBe(1 << 20); // 1MB
    expect(DEFAULT_MAX_FIELDS).toBe(1000);
    expect(DEFAULT_MAX_ERRORS).toBe(100);
  });

  test('buffer size limit exceeded', () => {
    const registry = new ToolRegistry();
    registry.register({ name: 'test', args: {} });

    const validator = new StreamingValidator(registry, {
      maxBufferSize: 50,
    });

    // Push more than 50 characters
    validator.pushToken('{action=test ');
    validator.pushToken('a'.repeat(100));

    expect(validator.shouldStop()).toBe(true);
    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.LimitExceeded)).toBe(true);
  });

  test('field count limit exceeded', () => {
    const registry = new ToolRegistry();
    registry.register({ name: 'test', args: {} });

    const validator = new StreamingValidator(registry, {
      maxFieldCount: 3,
    });

    validator.pushToken('{action=test f1=1 f2=2 f3=3 f4=4 f5=5}');

    const result = validator.getResult();
    expect(result.errors.some(e => e.code === ErrorCode.LimitExceeded)).toBe(true);
  });

  test('error count limit respected', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: {
        a: { type: 'int', min: 100 },
        b: { type: 'int', min: 100 },
        c: { type: 'int', min: 100 },
        d: { type: 'int', min: 100 },
        e: { type: 'int', min: 100 },
      },
    });

    const validator = new StreamingValidator(registry, {
      maxErrorCount: 2,
    });

    // Each field will generate an error (value < min)
    validator.pushToken('{action=test a=1 b=2 c=3 d=4 e=5}');

    const result = validator.getResult();
    expect(result.errors.length).toBeLessThanOrEqual(2);
  });

  test('withLimits chaining', () => {
    const registry = new ToolRegistry();
    const validator = new StreamingValidator(registry)
      .withLimits({ maxBufferSize: 100 })
      .withLimits({ maxFieldCount: 50 });

    // Validator should still work
    validator.pushToken('{action=test}');
    expect(validator.getResult()).toBeDefined();
  });
});

// ============================================================
// Timing Instrumentation Tests
// ============================================================

describe('StreamingValidator - Timing', () => {
  test('records token and character counts', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{');
    validator.pushToken('action=search ');
    validator.pushToken('query="test"');
    const result = validator.pushToken('}');

    expect(result.tokenCount).toBe(4);
    expect(result.charCount).toBeGreaterThan(0);
  });

  test('records tool detection timing', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{');
    validator.pushToken('action=search ');
    const result = validator.pushToken('query="test"}');

    expect(result.toolDetectedAtToken).toBeGreaterThan(0);
    expect(result.toolDetectedAtTime).toBeGreaterThanOrEqual(0);
  });

  test('records completion timing', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    const result = validator.pushToken('{action=search query="test"}');

    expect(result.complete).toBe(true);
    expect(result.completeAtToken).toBeGreaterThan(0);
  });

  test('records error timing', () => {
    const registry = new ToolRegistry();
    const validator = new StreamingValidator(registry);

    const result = validator.pushToken('{action=unknown_tool}');

    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.firstErrorAtToken).toBeGreaterThan(0);
  });

  test('timeline events recorded', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=search query="test"}');
    const result = validator.getResult();

    expect(result.timeline.length).toBeGreaterThan(0);
    expect(result.timeline.some(e => e.event === 'TOOL_DETECTED')).toBe(true);
    expect(result.timeline.some(e => e.event === 'COMPLETE')).toBe(true);
  });
});

// ============================================================
// Reset Tests
// ============================================================

describe('StreamingValidator - Reset', () => {
  test('reset clears all state', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=search query="test"}');
    expect(validator.getResult().complete).toBe(true);

    validator.reset();

    const result = validator.getResult();
    expect(result.complete).toBe(false);
    expect(result.toolName).toBeNull();
    expect(Object.keys(result.fields).length).toBe(0);
    expect(result.errors.length).toBe(0);
    expect(result.tokenCount).toBe(0);
  });

  test('validator can be reused after reset', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=search query="first"}');
    validator.reset();
    validator.pushToken('{action=browse url="https://example.com"}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
    expect(result.toolName).toBe('browse');
    expect(result.fields.url).toBe('https://example.com');
  });
});

// ============================================================
// Default Registry Tests
// ============================================================

describe('defaultToolRegistry', () => {
  test('includes search tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('search')).toBe(true);
    expect(registry.get('search')?.args.query.required).toBe(true);
  });

  test('includes calculate tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('calculate')).toBe(true);
  });

  test('includes browse tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('browse')).toBe(true);
    expect(registry.get('browse')?.args.url.pattern).toBeDefined();
  });

  test('includes execute tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('execute')).toBe(true);
  });

  test('includes read_file tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('read_file')).toBe(true);
    expect(registry.get('read_file')?.args.path.required).toBe(true);
  });

  test('includes write_file tool', () => {
    const registry = defaultToolRegistry();
    expect(registry.isAllowed('write_file')).toBe(true);
    expect(registry.get('write_file')?.args.content.required).toBe(true);
  });
});

// ============================================================
// Edge Cases
// ============================================================

describe('StreamingValidator - Edge Cases', () => {
  test('empty input', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    const result = validator.getResult();
    expect(result.complete).toBe(false);
    expect(result.toolName).toBeNull();
  });

  test('nested braces in arrays', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: { items: { type: 'string' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test items=[1,2,3]}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
  });

  test('tool field as "tool" instead of "action"', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);
    validator.pushToken('{tool=search query="test"}');

    const result = validator.getResult();
    expect(result.toolName).toBe('search');
    expect(result.toolAllowed).toBe(true);
  });

  test('whitespace variations', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);
    validator.pushToken('{\n  action=search\n  query="test"\n}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
    expect(result.valid).toBe(true);
  });

  test('multiple tokens for single field', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=');
    validator.pushToken('se');
    validator.pushToken('ar');
    validator.pushToken('ch ');
    validator.pushToken('query="te');
    validator.pushToken('st"}');

    const result = validator.getResult();
    expect(result.toolName).toBe('search');
    expect(result.fields.query).toBe('test');
  });
});

// ============================================================
// Feature Parity Tests (Go/Python compatibility)
// ============================================================

describe('StreamingValidator - Feature Parity', () => {
  test('toolDetectedAtChar is recorded', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=search query="test"}');
    const result = validator.getResult();

    expect(result.toolDetectedAtChar).toBeGreaterThan(0);
  });

  test('state is included in result', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    let result = validator.getResult();
    expect(result.state).toBe(ValidatorState.Waiting);

    validator.pushToken('{action=search query="test"}');
    result = validator.getResult();
    expect(result.state).toBe(ValidatorState.Complete);
  });

  test('scientific notation parsing (1e5)', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: { value: { type: 'float' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test value=1e5}');

    const result = validator.getResult();
    expect(result.fields.value).toBe(100000);
  });

  test('scientific notation parsing (1.5e-3)', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: { value: { type: 'float' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test value=1.5e-3}');

    const result = validator.getResult();
    expect(result.fields.value).toBeCloseTo(0.0015);
  });

  test('Unicode null symbol (∅) parsing', () => {
    const registry = new ToolRegistry();
    registry.register({
      name: 'test',
      args: { value: { type: 'string' } },
    });

    const validator = new StreamingValidator(registry);
    validator.pushToken('{action=test value=∅}');

    const result = validator.getResult();
    expect(result.fields.value).toBeNull();
  });

  test('getParsed returns fields when complete and valid', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    expect(validator.getParsed()).toBeNull();

    validator.pushToken('{action=search query="test"}');

    const parsed = validator.getParsed();
    expect(parsed).not.toBeNull();
    expect(parsed?.query).toBe('test');
  });

  test('getParsed returns null when invalid', () => {
    const registry = new ToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('{action=unknown_tool}');

    expect(validator.getParsed()).toBeNull();
  });

  test('tool name before brace syntax (search{query=test})', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('search{query="test"}');

    const result = validator.getResult();
    expect(result.complete).toBe(true);
    expect(result.toolName).toBe('search');
    expect(result.toolAllowed).toBe(true);
    expect(result.fields.query).toBe('test');
  });

  test('tool name before brace - unknown tool', () => {
    const registry = new ToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('unknown_tool{param=value}');

    const result = validator.getResult();
    expect(result.toolName).toBe('unknown_tool');
    expect(result.toolAllowed).toBe(false);
    expect(result.errors.some(e => e.code === ErrorCode.UnknownTool)).toBe(true);
  });

  test('tool name before brace with spaces', () => {
    const registry = defaultToolRegistry();
    const validator = new StreamingValidator(registry);

    validator.pushToken('  search  {query="test"}');

    const result = validator.getResult();
    expect(result.toolName).toBe('search');
    expect(result.valid).toBe(true);
  });
});
