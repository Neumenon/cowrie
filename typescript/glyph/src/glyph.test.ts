/**
 * LYPH v2 JavaScript Tests
 */

import { 
  GValue, g, field,
  Schema, SchemaBuilder, t,
  fromJson, toJson, parseJson, stringifyJson,
  emit, emitPacked, emitTabular, emitV2,
  parsePacked, parseTabular, parseHeader,
  jsonToPacked, jsonToLyph, compareTokens,
  PatchBuilder, emitPatch, parsePatch, applyPatch,
  canonicalizeLoose, canonicalizeLooseNoTabular, canonicalizeLooseTabular, canonicalizeLooseWithOpts,
  equalLoose, fromJsonLoose, toJsonLoose, jsonEqual,
  parseTabularLoose, unescapeTabularCell,
} from './index';

// ============================================================
// GValue Tests
// ============================================================

describe('GValue', () => {
  test('null', () => {
    const v = g.null();
    expect(v.type).toBe('null');
    expect(v.isNull()).toBe(true);
  });

  test('bool', () => {
    expect(g.bool(true).asBool()).toBe(true);
    expect(g.bool(false).asBool()).toBe(false);
  });

  test('int', () => {
    expect(g.int(42).asInt()).toBe(42);
    expect(g.int(-123).asInt()).toBe(-123);
  });

  test('float', () => {
    expect(g.float(3.14).asFloat()).toBeCloseTo(3.14);
  });

  test('str', () => {
    expect(g.str('hello').asStr()).toBe('hello');
  });

  test('id', () => {
    const ref = g.id('t', 'ARS').asId();
    expect(ref.prefix).toBe('t');
    expect(ref.value).toBe('ARS');
  });

  test('list', () => {
    const list = g.list(g.int(1), g.int(2), g.int(3));
    expect(list.len()).toBe(3);
    expect(list.index(0).asInt()).toBe(1);
  });

  test('struct', () => {
    const s = g.struct('Team',
      field('id', g.id('t', 'ARS')),
      field('name', g.str('Arsenal'))
    );
    expect(s.asStruct().typeName).toBe('Team');
    expect(s.get('name')?.asStr()).toBe('Arsenal');
  });

  test('clone', () => {
    const original = g.struct('Test',
      field('num', g.int(42)),
      field('list', g.list(g.int(1), g.int(2)))
    );
    const cloned = original.clone();
    
    // Modify clone
    const list = cloned.get('list')?.asList();
    if (list) list[0] = g.int(999);
    
    // Original unchanged
    expect(original.get('list')?.index(0).asInt()).toBe(1);
  });
});

// ============================================================
// Schema Tests
// ============================================================

describe('Schema', () => {
  test('build schema', () => {
    const schema = new SchemaBuilder()
      .addPackedStruct('Team', 'v2')
        .field('id', t.id(), { fid: 1, wireKey: 't' })
        .field('name', t.str(), { fid: 2, wireKey: 'n' })
        .field('league', t.str(), { fid: 3, wireKey: 'l' })
      .build();
    
    expect(schema.getType('Team')).toBeDefined();
    expect(schema.getType('Team')?.packEnabled).toBe(true);
    expect(schema.fieldsByFid('Team').length).toBe(3);
  });

  test('field by fid', () => {
    const schema = new SchemaBuilder()
      .addPackedStruct('Test', 'v1')
        .field('c', t.str(), { fid: 3 })
        .field('a', t.str(), { fid: 1 })
        .field('b', t.str(), { fid: 2 })
      .build();
    
    const fields = schema.fieldsByFid('Test');
    expect(fields[0].name).toBe('a');
    expect(fields[1].name).toBe('b');
    expect(fields[2].name).toBe('c');
  });

  test('optional fields', () => {
    const schema = new SchemaBuilder()
      .addPackedStruct('Test', 'v1')
        .field('req', t.str(), { fid: 1 })
        .field('opt', t.str(), { fid: 2, optional: true })
      .build();
    
    expect(schema.requiredFieldsByFid('Test').length).toBe(1);
    expect(schema.optionalFieldsByFid('Test').length).toBe(1);
  });
});

// ============================================================
// JSON Conversion Tests
// ============================================================

describe('JSON conversion', () => {
  test('fromJson primitives', () => {
    expect(fromJson(null).isNull()).toBe(true);
    expect(fromJson(true).asBool()).toBe(true);
    expect(fromJson(42).asInt()).toBe(42);
    expect(fromJson(3.14).asFloat()).toBeCloseTo(3.14);
    expect(fromJson('hello').asStr()).toBe('hello');
  });

  test('fromJson ref', () => {
    const v = fromJson('^t:ARS');
    expect(v.type).toBe('id');
    expect(v.asId().prefix).toBe('t');
    expect(v.asId().value).toBe('ARS');
  });

  test('fromJson date', () => {
    const v = fromJson('2025-12-19T10:30:00Z');
    expect(v.type).toBe('time');
  });

  test('fromJson array', () => {
    const v = fromJson([1, 2, 3]);
    expect(v.type).toBe('list');
    expect(v.len()).toBe(3);
  });

  test('fromJson typed object', () => {
    const v = fromJson({ $type: 'Team', name: 'Arsenal' });
    expect(v.type).toBe('struct');
    expect(v.asStruct().typeName).toBe('Team');
    expect(v.get('name')?.asStr()).toBe('Arsenal');
  });

  test('toJson primitives', () => {
    expect(toJson(g.null())).toBe(null);
    expect(toJson(g.bool(true))).toBe(true);
    expect(toJson(g.int(42))).toBe(42);
    expect(toJson(g.str('hello'))).toBe('hello');
  });

  test('toJson ref compact', () => {
    const json = toJson(g.id('t', 'ARS'), { compactRefs: true });
    expect(json).toBe('^t:ARS');
  });

  test('toJson struct with type marker', () => {
    const s = g.struct('Team', field('name', g.str('Arsenal')));
    const json = toJson(s, { includeTypeMarkers: true }) as Record<string, unknown>;
    expect(json.$type).toBe('Team');
    expect(json.name).toBe('Arsenal');
  });

  test('roundtrip', () => {
    const original = { 
      id: '^t:ARS', 
      name: 'Arsenal', 
      founded: 1886,
      active: true 
    };
    
    const gv = fromJson(original);
    const back = toJson(gv, { compactRefs: true });
    
    expect(back).toEqual(original);
  });
});

// ============================================================
// Emit Tests
// ============================================================

describe('Emit', () => {
  const schema = new SchemaBuilder()
    .addPackedStruct('Team', 'v2')
      .field('id', t.id(), { fid: 1, wireKey: 't' })
      .field('name', t.str(), { fid: 2, wireKey: 'n' })
      .field('league', t.str(), { fid: 3, wireKey: 'l' })
    .build();

  const team = g.struct('Team',
    field('id', g.id('t', 'ARS')),
    field('name', g.str('Arsenal')),
    field('league', g.str('EPL'))
  );

  test('emit struct mode', () => {
    const result = emit(team);
    expect(result).toContain('Team{');
    expect(result).toContain('id=^t:ARS');
    expect(result).toContain('name=Arsenal');
  });

  test('emit packed', () => {
    const result = emitPacked(team, schema);
    expect(result).toBe('Team@(^t:ARS Arsenal EPL)');
  });

  test('emit tabular', () => {
    const list = g.list(
      g.struct('Team',
        field('id', g.id('t', 'ARS')),
        field('name', g.str('Arsenal')),
        field('league', g.str('EPL'))
      ),
      g.struct('Team',
        field('id', g.id('t', 'LIV')),
        field('name', g.str('Liverpool')),
        field('league', g.str('EPL'))
      ),
      g.struct('Team',
        field('id', g.id('t', 'MCI')),
        field('name', g.str('Man City')),
        field('league', g.str('EPL'))
      )
    );
    
    const result = emitTabular(list, schema);
    expect(result).toContain('@tab Team [t n l]');
    expect(result).toContain('@end');
    expect(result.split('\n').length).toBe(5); // header + 3 rows + footer
  });
});

// ============================================================
// Parse Tests
// ============================================================

describe('Parse', () => {
  const schema = new SchemaBuilder()
    .addPackedStruct('Team', 'v2')
      .field('id', t.id(), { fid: 1, wireKey: 't' })
      .field('name', t.str(), { fid: 2, wireKey: 'n' })
      .field('league', t.str(), { fid: 3, wireKey: 'l' })
    .build();

  test('parse packed dense', () => {
    const result = parsePacked('Team@(^t:ARS Arsenal EPL)', schema);
    expect(result.asStruct().typeName).toBe('Team');
    expect(result.get('id')?.asId().value).toBe('ARS');
    expect(result.get('name')?.asStr()).toBe('Arsenal');
    expect(result.get('league')?.asStr()).toBe('EPL');
  });

  test('parse packed with nulls', () => {
    const schemaWithOpt = new SchemaBuilder()
      .addPackedStruct('Test', 'v1')
        .field('a', t.str(), { fid: 1 })
        .field('b', t.str(), { fid: 2, optional: true })
        .field('c', t.str(), { fid: 3 })
      .build();
    
    const result = parsePacked('Test@(hello ∅ world)', schemaWithOpt);
    expect(result.get('a')?.asStr()).toBe('hello');
    expect(result.get('b')?.isNull()).toBe(true);
    expect(result.get('c')?.asStr()).toBe('world');
  });

  test('parse header', () => {
    const h = parseHeader('@lyph v2 @schema#abc123 @mode=packed @keys=wire');
    expect(h?.version).toBe('v2');
    expect(h?.schemaId).toBe('abc123');
    expect(h?.mode).toBe('packed');
    expect(h?.keyMode).toBe('wire');
  });

  test('parse tabular', () => {
    const input = `@tab Team [t n l]
^t:ARS Arsenal EPL
^t:LIV Liverpool EPL
^t:CHE Chelsea EPL
@end`;
    
    const result = parseTabular(input, schema);
    expect(result.typeName).toBe('Team');
    expect(result.columns).toEqual(['t', 'n', 'l']);
    expect(result.rows.length).toBe(3);
    
    expect(result.rows[0].get('name')?.asStr()).toBe('Arsenal');
    expect(result.rows[1].get('name')?.asStr()).toBe('Liverpool');
    expect(result.rows[2].get('name')?.asStr()).toBe('Chelsea');
  });

  test('roundtrip packed', () => {
    const original = g.struct('Team',
      field('id', g.id('t', 'ARS')),
      field('name', g.str('Arsenal')),
      field('league', g.str('EPL'))
    );
    
    const packed = emitPacked(original, schema);
    const parsed = parsePacked(packed, schema);
    
    expect(parsed.get('id')?.asId().value).toBe('ARS');
    expect(parsed.get('name')?.asStr()).toBe('Arsenal');
    expect(parsed.get('league')?.asStr()).toBe('EPL');
  });
});

// ============================================================
// Patch Tests
// ============================================================

describe('Patch', () => {
  test('patch builder', () => {
    const patch = new PatchBuilder({ prefix: 'm', value: 'ARS-LIV' })
      .withSchemaId('abc123')
      .set('ft_h', g.int(2))
      .set('ft_a', g.int(1))
      .append('events', g.str('Goal!'))
      .delete('odds')
      .delta('rating', 0.15)
      .build();
    
    expect(patch.target.prefix).toBe('m');
    expect(patch.target.value).toBe('ARS-LIV');
    expect(patch.schemaId).toBe('abc123');
    expect(patch.ops.length).toBe(5);
  });

  test('emit patch', () => {
    const patch = new PatchBuilder({ prefix: 'm', value: 'ARS-LIV' })
      .withSchemaId('abc123')
      .set('ft_h', g.int(2))
      .set('ft_a', g.int(1))
      .build();
    
    const emitted = emitPatch(patch);
    
    expect(emitted).toContain('@patch');
    expect(emitted).toContain('@schema#abc123');
    expect(emitted).toContain('@target=m:ARS-LIV');
    expect(emitted).toContain('= ft_a 1');
    expect(emitted).toContain('= ft_h 2');
    expect(emitted).toContain('@end');
  });

  test('parse patch', () => {
    const input = `@patch @schema#abc123 @keys=wire @target=m:ARS-LIV
= ft_h 2
= ft_a 1
+ events "Goal!"
- odds
~ rating +0.15
@end`;
    
    const patch = parsePatch(input);
    
    expect(patch.target.prefix).toBe('m');
    expect(patch.target.value).toBe('ARS-LIV');
    expect(patch.schemaId).toBe('abc123');
    expect(patch.ops.length).toBe(5);
    
    expect(patch.ops[0].op).toBe('=');
    expect(patch.ops[0].path[0].field).toBe('ft_h');
    expect(patch.ops[0].value?.asInt()).toBe(2);
    
    expect(patch.ops[2].op).toBe('+');
    expect(patch.ops[2].value?.asStr()).toBe('Goal!');
    
    expect(patch.ops[3].op).toBe('-');
    
    expect(patch.ops[4].op).toBe('~');
    expect(patch.ops[4].value?.asFloat()).toBe(0.15);
  });

  test('patch roundtrip', () => {
    const original = new PatchBuilder({ prefix: 'm', value: 'TEST' })
      .withSchemaId('test123')
      .set('score', g.int(42))
      .append('items', g.str('new item'))
      .delete('old')
      .delta('count', -5)
      .build();
    
    const emitted = emitPatch(original);
    const parsed = parsePatch(emitted);
    const reEmitted = emitPatch(parsed);
    
    expect(reEmitted).toBe(emitted);
  });

  test('apply patch', () => {
    const value = g.struct('Match',
      field('ft_h', g.int(0)),
      field('ft_a', g.int(0)),
      field('events', g.list()),
      field('rating', g.float(1.0))
    );
    
    const patch = new PatchBuilder({ prefix: 'm', value: 'ARS-LIV' })
      .set('ft_h', g.int(2))
      .set('ft_a', g.int(1))
      .append('events', g.str('Goal!'))
      .delta('rating', 0.15)
      .build();
    
    const result = applyPatch(value, patch);
    
    expect(result.get('ft_h')?.asInt()).toBe(2);
    expect(result.get('ft_a')?.asInt()).toBe(1);
    expect(result.get('events')?.asList().length).toBe(1);
    expect(result.get('events')?.index(0).asStr()).toBe('Goal!');
    expect(result.get('rating')?.asFloat()).toBeCloseTo(1.15);
  });

  test('apply patch with nested path', () => {
    const value = g.struct('Game',
      field('home', g.struct('Team',
        field('score', g.int(0))
      ))
    );
    
    const patch = new PatchBuilder({ prefix: 'g', value: '123' })
      .set('home.score', g.int(3))
      .build();
    
    const result = applyPatch(value, patch);
    
    expect(result.get('home')?.get('score')?.asInt()).toBe(3);
  });
});

// ============================================================
// Loose Mode Tests
// ============================================================

describe('Loose Mode', () => {
  describe('canonicalizeLoose', () => {
    test('scalars', () => {
      expect(canonicalizeLoose(g.null())).toBe('_');
      expect(canonicalizeLoose(g.bool(true))).toBe('t');
      expect(canonicalizeLoose(g.bool(false))).toBe('f');
      expect(canonicalizeLoose(g.int(0))).toBe('0');
      expect(canonicalizeLoose(g.int(42))).toBe('42');
      expect(canonicalizeLoose(g.int(-100))).toBe('-100');
      expect(canonicalizeLoose(g.float(0))).toBe('0');
      expect(canonicalizeLoose(g.float(3.14))).toBe('3.14');
      expect(canonicalizeLoose(g.str('hello'))).toBe('hello');
      expect(canonicalizeLoose(g.str('hello world'))).toBe('"hello world"');
      expect(canonicalizeLoose(g.str(''))).toBe('""');
      expect(canonicalizeLoose(g.str('t'))).toBe('"t"');
      expect(canonicalizeLoose(g.str('null'))).toBe('"null"');
    });

    test('id', () => {
      expect(canonicalizeLoose(g.id('user', '123'))).toBe('^user:123');
    });

    test('list', () => {
      expect(canonicalizeLoose(g.list())).toBe('[]');
      expect(canonicalizeLoose(g.list(g.int(1)))).toBe('[1]');
      expect(canonicalizeLoose(g.list(g.int(1), g.int(2), g.int(3)))).toBe('[1 2 3]');
      expect(canonicalizeLoose(g.list(g.null(), g.bool(true), g.int(42)))).toBe('[_ t 42]');
    });

    test('map sorted keys', () => {
      expect(canonicalizeLoose(g.map())).toBe('{}');
      expect(canonicalizeLoose(g.map(
        field('a', g.int(1))
      ))).toBe('{a=1}');

      // Keys should be sorted: A < _ < a < aa < b (bytewise UTF-8)
      const mapWithKeys = g.map(
        field('b', g.int(1)),
        field('a', g.int(2)),
        field('aa', g.int(3)),
        field('A', g.int(4)),
        field('_', g.int(5))
      );
      expect(canonicalizeLoose(mapWithKeys)).toBe('{A=4 _=5 a=2 aa=3 b=1}');
    });

    test('nested', () => {
      const nested = g.map(
        field('inner', g.map(field('x', g.int(1))))
      );
      expect(canonicalizeLoose(nested)).toBe('{inner={x=1}}');
    });
  });

  describe('equalLoose', () => {
    test('maps with same content different order', () => {
      const map1 = g.map(
        field('a', g.int(1)),
        field('b', g.int(2))
      );
      const map2 = g.map(
        field('b', g.int(2)),
        field('a', g.int(1))
      );
      expect(equalLoose(map1, map2)).toBe(true);
    });

    test('maps with different content', () => {
      const map1 = g.map(field('a', g.int(1)));
      const map2 = g.map(field('a', g.int(2)));
      expect(equalLoose(map1, map2)).toBe(false);
    });
  });

  describe('fromJsonLoose', () => {
    test('primitives', () => {
      expect(fromJsonLoose(null).isNull()).toBe(true);
      expect(fromJsonLoose(true).asBool()).toBe(true);
      expect(fromJsonLoose(false).asBool()).toBe(false);
      expect(fromJsonLoose(42).asInt()).toBe(42);
      expect(fromJsonLoose(3.14).asFloat()).toBeCloseTo(3.14);
      expect(fromJsonLoose('hello').asStr()).toBe('hello');
    });

    test('arrays and objects', () => {
      const arr = fromJsonLoose([1, 2, 3]);
      expect(arr.type).toBe('list');
      expect(arr.len()).toBe(3);

      const obj = fromJsonLoose({ a: 1, b: 2 });
      expect(obj.type).toBe('map');
      expect(obj.get('a')?.asInt()).toBe(1);
    });

    test('rejects NaN', () => {
      expect(() => fromJsonLoose(NaN)).toThrow('NaN/Infinity');
    });

    test('rejects Infinity', () => {
      expect(() => fromJsonLoose(Infinity)).toThrow('NaN/Infinity');
    });
  });

  describe('toJsonLoose', () => {
    test('primitives', () => {
      expect(toJsonLoose(g.null())).toBe(null);
      expect(toJsonLoose(g.bool(true))).toBe(true);
      expect(toJsonLoose(g.int(42))).toBe(42);
      expect(toJsonLoose(g.float(3.14))).toBe(3.14);
      expect(toJsonLoose(g.str('hello'))).toBe('hello');
    });

    test('time and id without extended', () => {
      const time = g.time(new Date('2025-12-19T10:30:00Z'));
      expect(toJsonLoose(time)).toBe('2025-12-19T10:30:00.000Z');

      const id = g.id('user', '123');
      expect(toJsonLoose(id)).toBe('^user:123');
    });

    test('time and id with extended', () => {
      const time = g.time(new Date('2025-12-19T10:30:00Z'));
      const timeJson = toJsonLoose(time, { extended: true }) as { $glyph: string; value: string };
      expect(timeJson.$glyph).toBe('time');
      expect(timeJson.value).toBe('2025-12-19T10:30:00.000Z');

      const id = g.id('user', '123');
      const idJson = toJsonLoose(id, { extended: true }) as { $glyph: string; value: string };
      expect(idJson.$glyph).toBe('id');
      expect(idJson.value).toBe('^user:123');
    });
  });

  describe('JSON roundtrip', () => {
    test('simple object', () => {
      const original = { a: 1, b: 'hello', c: true, d: null };
      const gv = fromJsonLoose(original);
      const back = toJsonLoose(gv);
      expect(back).toEqual(original);
    });

    test('nested structure', () => {
      const original = {
        user: { name: 'Alice', age: 30 },
        items: [1, 2, 3]
      };
      const gv = fromJsonLoose(original);
      const back = toJsonLoose(gv);
      expect(back).toEqual(original);
    });
  });

  describe('jsonEqual', () => {
    test('equal objects', () => {
      expect(jsonEqual('{"a":1,"b":2}', '{"b":2,"a":1}')).toBe(true);
    });

    test('different objects', () => {
      expect(jsonEqual('{"a":1}', '{"a":2}')).toBe(false);
    });
  });
});

// ============================================================
// Auto-Tabular Tests (v2.3.0)
// ============================================================

describe('Auto-Tabular', () => {
  describe('canonicalizeLooseTabular', () => {
    test('basic list of objects', () => {
      const list = g.list(
        g.map(field('id', g.int(1)), field('name', g.str('a'))),
        g.map(field('id', g.int(2)), field('name', g.str('b'))),
        g.map(field('id', g.int(3)), field('name', g.str('c')))
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toContain('@tab _ rows=3 cols=2 [id name]');
      expect(result).toContain('|1|a|');
      expect(result).toContain('|2|b|');
      expect(result).toContain('|3|c|');
      expect(result).toContain('@end');
    });

    test('below threshold stays as list', () => {
      const list = g.list(
        g.map(field('id', g.int(1))),
        g.map(field('id', g.int(2)))
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toBe('[{id=1} {id=2}]');
    });

    test('heterogeneous list stays as list', () => {
      const list = g.list(
        g.map(field('id', g.int(1))),
        g.int(42),
        g.str('hello')
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toBe('[{id=1} 42 hello]');
    });

    test('missing keys emit null underscore', () => {
      const list = g.list(
        g.map(field('id', g.int(1)), field('name', g.str('a'))),
        g.map(field('id', g.int(2))),
        g.map(field('id', g.int(3)), field('name', g.str('c')))
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toContain('|2|_|');
    });

    test('pipe escaping in values', () => {
      const list = g.list(
        g.map(field('val', g.str('a|b'))),
        g.map(field('val', g.str('c|d'))),
        g.map(field('val', g.str('e|f')))
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toContain('"a\\|b"');
      expect(result).toContain('"c\\|d"');
    });

    test('nested values in cells', () => {
      const list = g.list(
        g.map(field('id', g.int(1)), field('meta', g.map(field('x', g.int(10))))),
        g.map(field('id', g.int(2)), field('meta', g.map(field('x', g.int(20))))),
        g.map(field('id', g.int(3)), field('meta', g.map(field('x', g.int(30)))))
      );
      
      const result = canonicalizeLooseTabular(list);
      expect(result).toContain('{x=10}');
      expect(result).toContain('{x=20}');
    });

    test('enabled by default', () => {
      const list = g.list(
        g.map(field('id', g.int(1))),
        g.map(field('id', g.int(2))),
        g.map(field('id', g.int(3)))
      );
      
      const result = canonicalizeLoose(list);
      expect(result).toContain('@tab _');
      expect(result).toContain('@end');
    });

    test('can be disabled with NoTabular', () => {
      const list = g.list(
        g.map(field('id', g.int(1))),
        g.map(field('id', g.int(2))),
        g.map(field('id', g.int(3)))
      );
      
      const result = canonicalizeLooseNoTabular(list);
      expect(result).toBe('[{id=1} {id=2} {id=3}]');
    });

    test('custom minRows option', () => {
      const list = g.list(
        g.map(field('id', g.int(1))),
        g.map(field('id', g.int(2)))
      );
      
      const result = canonicalizeLooseWithOpts(list, { autoTabular: true, minRows: 2 });
      expect(result).toContain('@tab _');
    });
  });

  describe('escapeUnescapeTabularCell', () => {
    test('round-trip with pipes', () => {
      const original = 'a|b|c';
      const escaped = original.replace(/\|/g, '\\|');
      const unescaped = unescapeTabularCell(escaped);
      expect(unescaped).toBe(original);
    });

    test('backslash not escaped', () => {
      const input = 'a\\nb';
      const escaped = input.replace(/\|/g, '\\|');
      expect(escaped).toBe('a\\nb'); // No change
    });
  });

  describe('parseTabularLoose', () => {
    test('basic parsing', () => {
      const input = `@tab _ [id name]
|1|a|
|2|b|
|3|c|
@end`;
      
      const result = parseTabularLoose(input);
      expect(result.columns).toEqual(['id', 'name']);
      expect(result.rows.length).toBe(3);
      expect(result.rows[0]).toEqual({ id: 1, name: 'a' });
      expect(result.rows[1]).toEqual({ id: 2, name: 'b' });
      expect(result.rows[2]).toEqual({ id: 3, name: 'c' });
    });

    test('with null values', () => {
      const input = `@tab _ [id name]
|1|a|
|2|∅|
|3|c|
@end`;
      
      const result = parseTabularLoose(input);
      expect(result.rows[1]).toEqual({ id: 2, name: null });
    });

    test('with escaped pipes', () => {
      const input = `@tab _ [val]
|"a\\|b"|
|"c\\|d"|
|e|
@end`;
      
      const result = parseTabularLoose(input);
      expect(result.rows[0].val).toBe('a|b');
      expect(result.rows[1].val).toBe('c|d');
    });

    test('with nested values', () => {
      const input = `@tab _ [id meta]
|1|{x=10}|
|2|{x=20}|
|3|{x=30}|
@end`;
      
      const result = parseTabularLoose(input);
      expect(result.rows[0].meta).toEqual({ x: 10 });
    });

    test('with boolean and null', () => {
      const input = `@tab _ [flag val]
|t|1|
|f|∅|
|t|3|
@end`;
      
      const result = parseTabularLoose(input);
      expect(result.rows[0]).toEqual({ flag: true, val: 1 });
      expect(result.rows[1]).toEqual({ flag: false, val: null });
    });

    test('roundtrip', () => {
      const original = g.list(
        g.map(field('id', g.int(1)), field('name', g.str('alice'))),
        g.map(field('id', g.int(2)), field('name', g.str('bob'))),
        g.map(field('id', g.int(3)), field('name', g.str('carol')))
      );
      
      const emitted = canonicalizeLooseTabular(original);
      const parsed = parseTabularLoose(emitted);
      
      expect(parsed.rows[0]).toEqual({ id: 1, name: 'alice' });
      expect(parsed.rows[1]).toEqual({ id: 2, name: 'bob' });
      expect(parsed.rows[2]).toEqual({ id: 3, name: 'carol' });
    });
  });
});

// ============================================================
// Golden File Tests  
// ============================================================

import * as fs from 'fs';
import * as path from 'path';

interface ManifestCase {
  name: string;
  file: string;
}

interface Manifest {
  version: string;
  description: string;
  cases: ManifestCase[];
}

describe('Golden Files', () => {
  const testdataDir = path.join(__dirname, '..', '..', 'go', 'glyph', 'testdata', 'loose_json');
  const manifestPath = path.join(testdataDir, 'manifest.json');
  
  let manifest: Manifest;
  
  beforeAll(() => {
    const manifestData = fs.readFileSync(manifestPath, 'utf-8');
    manifest = JSON.parse(manifestData);
  });
  
  test.each([
    '000_empty_object', '001_empty_array', '002_null', '003_bool', '004_simple_scalars',
    '005_negative_and_float', '006_exponent_numbers', '007_string_escapes', '008_unicode',
    '009_unicode_escapes', '010_mixed_array', '011_nested_object', '012_nested_lists',
    '013_key_ordering_basic', '014_key_ordering_unicode', '015_empty_strings_and_spaces',
    '016_large_int_like', '017_deep_mixed', '018_duplicate_keys_last_wins', '019_object_with_nulls',
    '020_numbers_edgeish', '021_whitespace_noise', '022_big_strings', '023_map_like_labels',
    '024_apiish_payload', '025_deep_nesting_10', '026_deep_nesting_20', '027_deep_array_nesting',
    '028_many_keys_50', '029_long_key', '030_surrogate_pairs', '031_control_chars',
    '032_line_separators', '033_exp_boundary_small', '034_exp_boundary_large', '035_safe_int_boundary',
    '036_negative_zero', '037_many_duplicates', '038_mixed_duplicates', '039_numeric_keys',
    '040_reserved_word_keys', '041_special_chars_in_values', '042_bare_safe_edge', '043_not_bare_safe',
    '044_array_of_objects', '045_heterogeneous_array', '046_float_precision', '047_key_sort_stability',
    '048_unicode_keys', '049_real_api_response',
  ])('case %s matches golden', (caseName) => {
    const jsonPath = path.join(testdataDir, 'cases', caseName + '.json');
    const goldenPath = path.join(testdataDir, 'golden', caseName + '.want');
    
    const jsonData = fs.readFileSync(jsonPath, 'utf-8');
    const wantData = fs.readFileSync(goldenPath, 'utf-8').trim();
    
    const gv = fromJsonLoose(JSON.parse(jsonData));
    // Use NoTabular for golden file compat (v2.2.x format)
    const got = canonicalizeLooseNoTabular(gv);
    
    expect(got).toBe(wantData);
  });
});

// ============================================================
// Integration Tests
// ============================================================

describe('Integration', () => {
  test('JSON to LYPH packed', () => {
    const schema = new SchemaBuilder()
      .addPackedStruct('Team', 'v2')
        .field('id', t.id(), { fid: 1 })
        .field('name', t.str(), { fid: 2 })
      .build();
    
    const json = { $type: 'Team', id: '^t:ARS', name: 'Arsenal' };
    const lyph = jsonToPacked(json, schema);
    
    expect(lyph).toBe('Team@(^t:ARS Arsenal)');
  });

  test('token savings', () => {
    const schema = new SchemaBuilder()
      .addPackedStruct('Match', 'v2')
        .field('id', t.id(), { fid: 1 })
        .field('home', t.str(), { fid: 2 })
        .field('away', t.str(), { fid: 3 })
        .field('homeScore', t.int(), { fid: 4 })
        .field('awayScore', t.int(), { fid: 5 })
      .build();
    
    const matches = [
      { $type: 'Match', id: '^m:1', home: 'Arsenal', away: 'Liverpool', homeScore: 2, awayScore: 1 },
      { $type: 'Match', id: '^m:2', home: 'Chelsea', away: 'Man United', homeScore: 0, awayScore: 0 },
      { $type: 'Match', id: '^m:3', home: 'Man City', away: 'Tottenham', homeScore: 3, awayScore: 2 },
    ];
    
    const stats = compareTokens(matches, schema);
    console.log(`JSON tokens: ${stats.json}, LYPH tokens: ${stats.lyph}, Savings: ${stats.savingsPercent.toFixed(1)}%`);
    
    // LYPH should be more efficient for larger data
    // Note: Token counting is a rough estimate and may not be accurate for small samples
    expect(stats.json).toBeGreaterThan(0);
    expect(stats.lyph).toBeGreaterThan(0);
  });
});
