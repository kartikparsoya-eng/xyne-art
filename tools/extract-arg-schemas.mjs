#!/usr/bin/env node
// extract-arg-schemas.mjs — per-query/mutator Zod ARG SCHEMA extractor.
//
// Idea adopted from xyne-spaces feature/art scripts/staging-regression
// (auto-fixtures.mjs synthesizes fixture args straight from source Zod
// schemas). This is xyne-art's own compact implementation: it TS-AST-parses
// the DEPLOYED backend image's src/zero/{queries,mutators}.ts (the exact
// schemas the server validates transforms against — the file a stale scalar
// like viewMode:"kanban" gets rejected by) and serializes every defineQuery/
// defineMutator first-arg Zod schema to JSON. z.nativeEnum(X) identifiers are
// resolved to their VALUES at runtime via @prisma/client (available inside
// the container); unresolvable ones keep the enum name so gen_id_pool_db.py
// can fall back to pg_enum.
//
// Run INSIDE the backend container (tools/gen_arg_schemas.sh):
//   node extract-arg-schemas.mjs --queries src/zero/queries.ts \
//        --mutators src/zero/mutators.ts --out /tmp/args.json
import {readFile, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import ts from 'typescript';

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].slice(2)] = process.argv[i + 1];
const Q_PATH = args.queries ?? 'src/zero/queries.ts';
const M_PATH = args.mutators ?? 'src/zero/mutators.ts';
const OUT = args.out ?? '/tmp/args.json';

// Chain methods that refine but don't change the base kind.
const MODIFIERS = new Set([
  'optional', 'nullable', 'nullish', 'default', 'describe', 'min', 'max',
  'trim', 'int', 'positive', 'nonnegative', 'negative', 'catch', 'refine',
  'superRefine', 'transform', 'readonly', 'strict', 'passthrough', 'partial',
  'length', 'email', 'url', 'uuid', 'regex', 'nonempty', 'gt', 'gte', 'lt',
  'lte', 'finite', 'safe', 'multipleOf', 'startsWith', 'endsWith', 'includes',
]);

function propName(name) {
  return name && (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name))
    ? name.text : null;
}

function serializeZod(node, consts) {
  if (!node) return {type: 'unknown'};
  if (ts.isParenthesizedExpression(node)) return serializeZod(node.expression, consts);
  if (ts.isIdentifier(node) && consts.has(node.text)) {
    return serializeZod(consts.get(node.text), consts);
  }
  const out = {};
  let cur = node;
  for (;;) {
    if (ts.isCallExpression(cur) && ts.isPropertyAccessExpression(cur.expression)
        && MODIFIERS.has(cur.expression.name.text)) {
      const m = cur.expression.name.text;
      if (m === 'optional' || m === 'nullish') out.optional = true;
      if (m === 'nullable' || m === 'nullish') out.nullable = true;
      if (m === 'default') out.hasDefault = true;
      cur = cur.expression.expression;
      continue;
    }
    break;
  }
  if (ts.isCallExpression(cur) && ts.isPropertyAccessExpression(cur.expression)) {
    const kind = cur.expression.name.text;
    switch (kind) {
      case 'object': {
        out.type = 'object';
        out.keys = {};
        const lit = cur.arguments[0];
        if (lit && ts.isObjectLiteralExpression(lit)) {
          for (const p of lit.properties) {
            if (!ts.isPropertyAssignment(p)) continue;
            const k = propName(p.name);
            if (k) out.keys[k] = serializeZod(p.initializer, consts);
          }
        }
        return out;
      }
      case 'enum': {
        out.type = 'enum';
        out.values = [];
        const arr = cur.arguments[0];
        if (arr && ts.isArrayLiteralExpression(arr)) {
          for (const el of arr.elements) {
            if (ts.isStringLiteralLike(el)) out.values.push(el.text);
          }
        }
        return out;
      }
      case 'nativeEnum': {
        out.type = 'nativeEnum';
        const id = cur.arguments[0];
        out.enum = id && ts.isIdentifier(id) ? id.text
          : id && ts.isPropertyAccessExpression(id) ? id.name.text : null;
        return out;
      }
      case 'array': {
        out.type = 'array';
        out.element = cur.arguments[0] ? serializeZod(cur.arguments[0], consts) : {type: 'unknown'};
        return out;
      }
      case 'union': {
        out.type = 'union';
        out.variants = [];
        const arr = cur.arguments[0];
        if (arr && ts.isArrayLiteralExpression(arr)) {
          for (const el of arr.elements) out.variants.push(serializeZod(el, consts));
        }
        return out;
      }
      case 'literal': {
        out.type = 'literal';
        const v = cur.arguments[0];
        out.value = v && ts.isStringLiteralLike(v) ? v.text
          : v && ts.isNumericLiteral(v) ? Number(v.text)
          : v && v.kind === ts.SyntaxKind.TrueKeyword ? true
          : v && v.kind === ts.SyntaxKind.FalseKeyword ? false : null;
        return out;
      }
      case 'string': case 'number': case 'boolean': case 'date':
      case 'bigint': case 'any': case 'unknown': case 'record': case 'tuple':
        out.type = kind;
        return out;
      default:
        out.type = 'opaque';
        out.src = cur.getText().slice(0, 120);
        return out;
    }
  }
  out.type = 'opaque';
  out.src = (cur.getText ? cur.getText() : '').slice(0, 120);
  return out;
}

// Dotted name from the PropertyAssignment ancestor chain (mutators nest:
// channel: { markChannelAsViewed: defineMutator(...) } -> channel.markChannelAsViewed).
function dottedName(prop) {
  const parts = [];
  for (let n = prop; n; n = n.parent) {
    if (ts.isPropertyAssignment(n)) {
      const t = propName(n.name);
      if (t) parts.unshift(t);
    }
  }
  return parts.join('.');
}

async function extract(path, calleeName) {
  let text;
  try {
    text = await readFile(path, 'utf8');
  } catch {
    return {entries: {}, enums: new Set(), missing: true};
  }
  const sf = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  // top-level `const X = <zod expr>` for shared-schema indirection
  const consts = new Map();
  for (const st of sf.statements) {
    if (!ts.isVariableStatement(st)) continue;
    for (const d of st.declarationList.declarations) {
      if (ts.isIdentifier(d.name) && d.initializer) consts.set(d.name.text, d.initializer);
    }
  }
  const entries = {};
  const enums = new Set();
  const visit = node => {
    if (ts.isPropertyAssignment(node) && ts.isCallExpression(node.initializer)) {
      const ex = node.initializer.expression;
      const callee = ts.isIdentifier(ex) ? ex.text
        : ts.isPropertyAccessExpression(ex) ? ex.name.text : null;
      if (callee === calleeName) {
        const name = dottedName(node);
        const schema = serializeZod(node.initializer.arguments[0], consts);
        const rec = {args: schema.type === 'object' ? schema.keys : null};
        if (schema.type !== 'object') rec.schema = schema;
        entries[name] = rec;
        JSON.stringify(rec, (k, v) => {
          if (k === 'enum' && typeof v === 'string') enums.add(v);
          return v;
        });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return {entries, enums, missing: false};
}

const q = await extract(Q_PATH, 'defineQuery');
const m = await extract(M_PATH, 'defineMutator');

// Resolve nativeEnum identifiers to values via @prisma/client (runtime truth).
const enums = {};
try {
  const require = createRequire(process.cwd() + '/');
  const prisma = require('@prisma/client');
  for (const name of new Set([...q.enums, ...m.enums])) {
    const val = prisma[name];
    if (val && typeof val === 'object') {
      const values = Object.values(val).filter(v => typeof v === 'string');
      if (values.length) enums[name] = values;
    }
  }
} catch { /* pg_enum fallback in gen_id_pool_db.py */ }

const doc = {
  generatedAt: new Date().toISOString(),
  source: {queries: Q_PATH, mutators: m.missing ? null : M_PATH},
  queries: q.entries,
  mutators: m.entries,
  enums,
};
await writeFile(OUT, JSON.stringify(doc, null, 1));
console.log(`wrote ${OUT}: ${Object.keys(q.entries).length} queries, `
  + `${Object.keys(m.entries).length} mutators, ${Object.keys(enums).length} enums resolved`);
