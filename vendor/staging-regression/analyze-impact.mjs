#!/usr/bin/env node
// VENDORED, UNMODIFIED, from xyne-spaces branch feature/art commit 81c133fa2
// (scripts/staging-regression/src/analyze-impact.mjs, author Chinmay Singh,
// XYNE-12332). TS-AST static analyzer: parses shared/src/zero/{schema,queries,
// mutators}.ts and emits per-query readTables (incl. related()/whereExists()
// resolved through schema relationships + helper recursion), per-mutator
// writeTables (tx.mutate.<table>.<op>), query-mutator overlap edges, and a
// per-table {queries,mutators} map. xyne-art runs it INSIDE the deployed
// backend container (tools/gen_impact_matrix.sh) so the matrix is authoritative
// for the image under test; matrix_oracle.py consumes the tables[] map for
// dark-table attribution. Re-vendor when upstream changes.
import {mkdir, readFile, readdir, writeFile} from 'node:fs/promises';
import {dirname, resolve} from 'node:path';
import ts from 'typescript';

const DEFAULT_REPO = '../..';
const DEFAULT_OUT = './inventories/generated/query-mutator-impact.generated.json';
const RELATION_METHODS = new Set(['related', 'whereExists']);

export async function buildImpactAnalysis({repoRoot = resolve(process.cwd(), DEFAULT_REPO), generatedAt = new Date().toISOString()} = {}) {
  const sourcePaths = {
    schema: resolve(repoRoot, 'shared/src/zero/schema.ts'),
    queries: resolve(repoRoot, 'shared/src/zero/queries.ts'),
    mutators: resolve(repoRoot, 'shared/src/zero/mutators.ts'),
  };
  const [schemaText, queriesText, mutatorsText] = await Promise.all([
    readFile(sourcePaths.schema, 'utf8'),
    readFile(sourcePaths.queries, 'utf8'),
    readFile(sourcePaths.mutators, 'utf8'),
  ]);
  const schemaSource = parseSource(sourcePaths.schema, schemaText);
  const queriesSource = parseSource(sourcePaths.queries, queriesText);
  const mutatorsSource = parseSource(sourcePaths.mutators, mutatorsText);
  const schema = analyzeSchema(schemaSource);
  const queryHelpers = collectTopLevelHelpers(queriesSource);
  const mutatorHelpers = collectTopLevelHelpers(mutatorsSource);
  const queries = analyzeQueries(queriesSource, schema, queryHelpers);
  const mutators = analyzeMutators(mutatorsSource, schema, mutatorHelpers);
  const mutatorUsage = await scanMutatorCallSites(repoRoot, new Set(mutators.map(mutator => mutator.mutatorName)));
  const pairs = buildPairs(queries, mutators);
  const queryImpactGroups = buildQueryImpactGroups(queries, mutators, pairs);
  return {
    schemaVersion: 1,
    generatedAt,
    sourceFiles: {
      schema: relative(repoRoot, sourcePaths.schema),
      queries: relative(repoRoot, sourcePaths.queries),
      mutators: relative(repoRoot, sourcePaths.mutators),
    },
    summary: {
      tableCount: schema.tableNames.length,
      relationshipCount: schema.relationshipCount,
      queryCount: queries.length,
      mutatorCount: mutators.length,
      detectedUsedMutatorCount: mutatorUsage.usedMutatorCount,
      detectedMutatorCallSiteCount: mutatorUsage.callSiteCount,
      queryImpactGroupCount: queryImpactGroups.length,
      queryMutatorEdgeCount: pairs.length,
      maxMutatorsForSingleQuery: queryImpactGroups.reduce((max, group) => Math.max(max, group.mutatorCount), 0),
      queriesWithoutReadTables: queries.filter(query => query.readTables.length === 0).map(query => query.queryName),
      mutatorsWithoutWriteTables: mutators.filter(mutator => mutator.writeTables.length === 0).map(mutator => mutator.mutatorName),
      lowConfidenceQueryCount: queries.filter(query => query.analysisWarnings.length > 0).length,
      lowConfidenceMutatorCount: mutators.filter(mutator => mutator.analysisWarnings.length > 0).length,
      exportedMutatorsWithoutDetectedCallSite: mutatorUsage.exportedMutatorsWithoutDetectedCallSite,
      detectedCallSitesWithoutExportedMutator: mutatorUsage.detectedCallSitesWithoutExportedMutator,
    },
    queries,
    mutators,
    mutatorUsage,
    queryImpactGroups,
    pairs,
    tables: summarizeTables(queries, mutators),
    notes: [
      'queryImpactGroups is the primary execution model: each query has one object containing every mutator that can affect that query.',
      'pairs is supporting edge metadata: one edge per queryName + mutatorName table-overlap.',
      'This is a static impact map. Runtime validity still depends on checked-in fixture args for each query and mutator.',
      'Imported helper bodies and dynamic relation names are recorded as low-confidence warnings when they cannot be resolved statically.',
    ],
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const repoRoot = resolve(process.cwd(), args.repo ?? DEFAULT_REPO);
  const out = resolve(process.cwd(), args.out ?? DEFAULT_OUT);
  const analysis = await buildImpactAnalysis({repoRoot});
  await mkdir(dirname(out), {recursive: true});
  await writeFile(out, `${JSON.stringify(analysis, null, 2)}\n`);
  console.log(`wrote query-mutator impact matrix: ${out}`);
  console.log(`queries=${analysis.summary.queryCount} mutators=${analysis.summary.mutatorCount} queryGroups=${analysis.summary.queryImpactGroupCount} queryMutatorEdges=${analysis.summary.queryMutatorEdgeCount}`);
}

function parseSource(path, text) {
  return ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
}

function analyzeSchema(sourceFile) {
  const tableVarToName = new Map();
  const tableNameToVar = new Map();
  const relationships = new Map();
  visit(sourceFile, node => {
    if (!ts.isVariableDeclaration(node) || !ts.isIdentifier(node.name) || !node.initializer) {
      return;
    }
    const tableCall = findCall(node.initializer, call => expressionName(call.expression) === 'table');
    if (tableCall && isStringLike(tableCall.arguments[0])) {
      const tableName = tableCall.arguments[0].text;
      tableVarToName.set(node.name.text, tableName);
      tableNameToVar.set(tableName, node.name.text);
      return;
    }
    const relationshipCall = findCall(node.initializer, call => expressionName(call.expression) === 'relationships');
    if (!relationshipCall) {
      return;
    }
    const sourceTableVar = identifierText(relationshipCall.arguments[0]);
    const sourceTable = tableVarToName.get(sourceTableVar);
    if (!sourceTable) {
      return;
    }
    const relationObject = relationshipObjectLiteral(relationshipCall.arguments[1]);
    if (!relationObject) {
      return;
    }
    const tableRelations = relationships.get(sourceTable) ?? new Map();
    for (const property of relationObject.properties) {
      if (!ts.isPropertyAssignment(property)) {
        continue;
      }
      const relationName = propertyNameText(property.name);
      const destSchema = findPropertyValue(property.initializer, 'destSchema');
      const destTableVar = identifierText(destSchema);
      const destTable = tableVarToName.get(destTableVar);
      if (relationName && destTable) {
        tableRelations.set(relationName, destTable);
      }
    }
    relationships.set(sourceTable, tableRelations);
  });
  return {
    tableVarToName,
    tableNameToVar,
    tableNames: [...tableNameToVar.keys()].sort(),
    relationships,
    relationshipCount: [...relationships.values()].reduce((sum, item) => sum + item.size, 0),
  };
}

function relationshipObjectLiteral(arg) {
  if (!arg || !ts.isArrowFunction(arg)) {
    return null;
  }
  if (ts.isObjectLiteralExpression(arg.body)) {
    return arg.body;
  }
  if (ts.isParenthesizedExpression(arg.body) && ts.isObjectLiteralExpression(arg.body.expression)) {
    return arg.body.expression;
  }
  return null;
}

function collectTopLevelHelpers(sourceFile) {
  const helpers = new Map();
  for (const statement of sourceFile.statements) {
    if (ts.isFunctionDeclaration(statement) && statement.name && statement.body) {
      helpers.set(statement.name.text, {
        name: statement.name.text,
        params: statement.parameters.map(parameterName),
        body: statement.body,
      });
      continue;
    }
    if (!ts.isVariableStatement(statement)) {
      continue;
    }
    for (const declaration of statement.declarationList.declarations) {
      if (
        ts.isIdentifier(declaration.name)
        && declaration.initializer
        && (ts.isArrowFunction(declaration.initializer) || ts.isFunctionExpression(declaration.initializer))
      ) {
        helpers.set(declaration.name.text, {
          name: declaration.name.text,
          params: declaration.initializer.parameters.map(parameterName),
          body: declaration.initializer.body,
        });
      }
    }
  }
  return helpers;
}

function analyzeQueries(sourceFile, schema, helpers) {
  const queriesObject = findExportedObject(sourceFile, 'queries', 'defineQueries');
  if (!queriesObject) {
    return [];
  }
  const queries = [];
  for (const property of queriesObject.properties) {
    if (!ts.isPropertyAssignment(property)) {
      continue;
    }
    const queryName = propertyNameText(property.name);
    const defineQueryCall = asCall(property.initializer, 'defineQuery');
    if (!queryName || !defineQueryCall) {
      continue;
    }
    const handler = [...defineQueryCall.arguments].reverse().find(isFunctionLike);
    const usage = collectUsage(handler?.body ?? defineQueryCall, {
      sourceFile,
      schema,
      helpers,
      paramTables: new Map(),
      callStack: [],
    });
    queries.push({
      queryName,
      loc: loc(sourceFile, property),
      readTables: sortedUnion(usage.directReadTables, usage.relatedReadTables),
      directReadTables: sorted(usage.directReadTables),
      relatedReadTables: sorted(usage.relatedReadTables),
      relationRefs: usage.relationRefs,
      helperRefs: sorted(usage.helperRefs),
      analysisWarnings: usage.warnings,
    });
  }
  return queries.sort((a, b) => a.queryName.localeCompare(b.queryName));
}

function analyzeMutators(sourceFile, schema, helpers) {
  const mutatorsObject = findExportedObject(sourceFile, 'mutators', 'defineMutators');
  if (!mutatorsObject) {
    return [];
  }
  const mutators = [];
  walkMutatorObject(mutatorsObject, [], (mutatorName, property, defineMutatorCall) => {
    const handler = [...defineMutatorCall.arguments].reverse().find(isFunctionLike);
    const usage = collectUsage(handler?.body ?? defineMutatorCall, {
      sourceFile,
      schema,
      helpers,
      paramTables: new Map(),
      callStack: [],
    });
    mutators.push({
      mutatorName,
      category: mutatorName.includes('.') ? mutatorName.split('.')[0] : 'root',
      loc: loc(sourceFile, property),
      writeTables: sorted(usage.writeTables),
      readTables: sortedUnion(usage.directReadTables, usage.relatedReadTables),
      directReadTables: sorted(usage.directReadTables),
      relatedReadTables: sorted(usage.relatedReadTables),
      mutateOps: usage.mutateOps,
      relationRefs: usage.relationRefs,
      helperRefs: sorted(usage.helperRefs),
      analysisWarnings: usage.warnings,
    });
  });
  return mutators.sort((a, b) => a.mutatorName.localeCompare(b.mutatorName));
}

function walkMutatorObject(objectLiteral, path, onMutator) {
  for (const property of objectLiteral.properties) {
    if (!ts.isPropertyAssignment(property)) {
      continue;
    }
    const name = propertyNameText(property.name);
    if (!name) {
      continue;
    }
    const defineMutatorCall = asCall(property.initializer, 'defineMutator');
    if (defineMutatorCall) {
      onMutator([...path, name].join('.'), property, defineMutatorCall);
      continue;
    }
    if (ts.isObjectLiteralExpression(property.initializer)) {
      walkMutatorObject(property.initializer, [...path, name], onMutator);
    }
  }
}

async function scanMutatorCallSites(repoRoot, exportedMutators) {
  const scanRoots = ['dashboard/src', 'shared/src', 'backend/src'];
  const files = [];
  for (const scanRoot of scanRoots) {
    files.push(...await listSourceFiles(resolve(repoRoot, scanRoot)));
  }
  const byMutator = new Map();
  const unknown = new Map();
  for (const file of files) {
    const text = await readFile(file, 'utf8');
    const sourceFile = ts.createSourceFile(
      file,
      text,
      ts.ScriptTarget.Latest,
      true,
      file.endsWith('.tsx') || file.endsWith('.jsx') ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
    );
    visit(sourceFile, node => {
      if (!ts.isCallExpression(node)) {
        return;
      }
      const mutatorName = mutatorFactoryName(node.expression);
      if (!mutatorName) {
        return;
      }
      const entry = {
        file,
        line: loc(sourceFile, node).line,
        column: loc(sourceFile, node).column,
      };
      if (exportedMutators.has(mutatorName)) {
        const bucket = byMutator.get(mutatorName) ?? {count: 0, samples: []};
        bucket.count++;
        if (bucket.samples.length < 20) {
          bucket.samples.push(entry);
        }
        byMutator.set(mutatorName, bucket);
      } else {
        const bucket = unknown.get(mutatorName) ?? {count: 0, samples: []};
        bucket.count++;
        if (bucket.samples.length < 20) {
          bucket.samples.push(entry);
        }
        unknown.set(mutatorName, bucket);
      }
    });
  }
  const usedMutators = [...byMutator.keys()].sort();
  const unknownMutators = [...unknown.keys()].sort();
  return {
    scannedRoots: scanRoots,
    scannedFileCount: files.length,
    usedMutatorCount: usedMutators.length,
    callSiteCount: [...byMutator.values()].reduce((sum, bucket) => sum + bucket.count, 0),
    usedMutators,
    exportedMutatorsWithoutDetectedCallSite: [...exportedMutators].filter(name => !byMutator.has(name)).sort(),
    detectedCallSitesWithoutExportedMutator: unknownMutators.map(mutatorName => ({
      mutatorName,
      count: unknown.get(mutatorName).count,
      samples: unknown.get(mutatorName).samples,
    })),
    callSitesByMutator: Object.fromEntries(
      [...byMutator.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([mutatorName, bucket]) => [mutatorName, {count: bucket.count, samples: bucket.samples}]),
    ),
    notes: [
      'This scan detects direct mutators.namespace.name(...) call sites.',
      'Aliased, computed, or indirectly passed mutator factories may not be detected.',
    ],
  };
}

async function listSourceFiles(root) {
  let entries;
  try {
    entries = await readdir(root, {withFileTypes: true});
  } catch {
    return [];
  }
  const files = [];
  for (const entry of entries) {
    const path = resolve(root, entry.name);
    if (entry.isDirectory()) {
      if (shouldSkipDirectory(entry.name)) {
        continue;
      }
      files.push(...await listSourceFiles(path));
      continue;
    }
    if (entry.isFile() && /\.(tsx?|jsx?|mjs|cjs)$/.test(entry.name) && !entry.name.endsWith('.d.ts')) {
      files.push(path);
    }
  }
  return files;
}

function shouldSkipDirectory(name) {
  return new Set([
    '.git',
    '.next',
    'build',
    'coverage',
    'dist',
    'generated',
    'node_modules',
  ]).has(name);
}

function mutatorFactoryName(expression) {
  const parts = propertyAccessParts(expression);
  if (parts.length < 3 || parts[0] !== 'mutators') {
    return null;
  }
  return parts.slice(1).join('.');
}

function propertyAccessParts(expression) {
  if (ts.isIdentifier(expression)) {
    return [expression.text];
  }
  if (ts.isPropertyAccessExpression(expression)) {
    return [...propertyAccessParts(expression.expression), expression.name.text];
  }
  return [];
}

function collectUsage(root, options) {
  const usage = {
    directReadTables: new Set(),
    relatedReadTables: new Set(),
    writeTables: new Set(),
    mutateOpsByKey: new Map(),
    relationRefs: [],
    helperRefs: new Set(),
    warnings: [],
  };

  const visitNode = node => {
    if (!node) {
      return;
    }

    const zqlTable = zqlTableName(node);
    if (zqlTable) {
      usage.directReadTables.add(zqlTable);
    }

    const mutateCall = txMutateCall(node);
    if (mutateCall) {
      usage.writeTables.add(mutateCall.table);
      const key = `${mutateCall.table}:${mutateCall.op}`;
      const item = usage.mutateOpsByKey.get(key) ?? {
        table: mutateCall.table,
        op: mutateCall.op,
        count: 0,
        samples: [],
      };
      item.count++;
      if (item.samples.length < 5) {
        item.samples.push(loc(options.sourceFile, node));
      }
      usage.mutateOpsByKey.set(key, item);
    }

    if (ts.isCallExpression(node)) {
      const related = relationCall(node, options);
      if (related) {
        const relationRecord = {
          method: related.method,
          sourceTable: related.sourceTable,
          relationName: related.relationName,
          destTable: related.destTable,
          loc: loc(options.sourceFile, node),
        };
        usage.relationRefs.push(relationRecord);
        if (related.destTable) {
          usage.relatedReadTables.add(related.destTable);
        } else {
          usage.warnings.push({
            type: 'unresolved-relation',
            message: `Could not resolve ${related.method}("${related.relationName}") from ${related.sourceTable ?? 'unknown source table'}`,
            loc: loc(options.sourceFile, node),
          });
        }
        visitNode(related.receiver);
        visitRelationCallback(related, usage, options);
        visitNonCallbackArgs(node, related.callback, visitNode);
        return;
      }

      const helperCall = helperCallInfo(node, options);
      if (helperCall) {
        usage.helperRefs.add(helperCall.name);
        const nested = collectUsage(helperCall.helper.body, {
          ...options,
          paramTables: helperCall.paramTables,
          callStack: [...options.callStack, helperCall.name],
        });
        mergeUsage(usage, nested);
        for (const arg of node.arguments) {
          visitNode(arg);
        }
        return;
      }
    }

    ts.forEachChild(node, visitNode);
  };

  visitNode(root);
  return {
    directReadTables: usage.directReadTables,
    relatedReadTables: usage.relatedReadTables,
    writeTables: usage.writeTables,
    mutateOps: [...usage.mutateOpsByKey.values()].sort((a, b) => `${a.table}:${a.op}`.localeCompare(`${b.table}:${b.op}`)),
    relationRefs: dedupeRelationRefs(usage.relationRefs),
    helperRefs: usage.helperRefs,
    warnings: dedupeWarnings(usage.warnings),
  };
}

function relationCall(node, options) {
  if (!ts.isPropertyAccessExpression(node.expression)) {
    return null;
  }
  const method = node.expression.name.text;
  if (!RELATION_METHODS.has(method)) {
    return null;
  }
  const relationArg = node.arguments[0];
  if (!isStringLike(relationArg)) {
    return null;
  }
  const receiver = node.expression.expression;
  const sourceTable = inferSourceTable(receiver, options);
  const relationName = relationArg.text;
  const destTable = sourceTable ? options.schema.relationships.get(sourceTable)?.get(relationName) : undefined;
  const callback = [...node.arguments].find(isFunctionLike);
  return {method, receiver, sourceTable, relationName, destTable, callback};
}

function visitRelationCallback(related, usage, options) {
  if (!related.callback || !related.destTable) {
    return;
  }
  const firstParam = parameterName(related.callback.parameters[0]);
  const paramTables = new Map(options.paramTables);
  if (firstParam) {
    paramTables.set(firstParam, related.destTable);
  }
  const nested = collectUsage(related.callback.body, {
    ...options,
    paramTables,
  });
  mergeUsage(usage, nested);
}

function visitNonCallbackArgs(node, callback, visitNode) {
  for (const arg of node.arguments) {
    if (arg === callback) {
      continue;
    }
    visitNode(arg);
  }
}

function helperCallInfo(node, options) {
  if (!ts.isIdentifier(node.expression)) {
    return null;
  }
  const name = node.expression.text;
  const helper = options.helpers.get(name);
  if (!helper || options.callStack.includes(name)) {
    return null;
  }
  const paramTables = new Map(options.paramTables);
  helper.params.forEach((paramName, index) => {
    if (!paramName) {
      return;
    }
    const inferred = inferSourceTable(node.arguments[index], options);
    if (inferred) {
      paramTables.set(paramName, inferred);
    }
  });
  return {name, helper, paramTables};
}

function inferSourceTable(expression, options) {
  if (!expression) {
    return null;
  }
  if (ts.isIdentifier(expression)) {
    return options.paramTables.get(expression.text) ?? null;
  }
  if (ts.isPropertyAccessExpression(expression)) {
    if (ts.isIdentifier(expression.expression) && expression.expression.text === 'zql') {
      return expression.name.text;
    }
    return inferSourceTable(expression.expression, options);
  }
  if (ts.isCallExpression(expression)) {
    if (ts.isPropertyAccessExpression(expression.expression)) {
      return inferSourceTable(expression.expression.expression, options);
    }
    return inferSourceTable(expression.expression, options);
  }
  if (ts.isParenthesizedExpression(expression)) {
    return inferSourceTable(expression.expression, options);
  }
  return null;
}

function zqlTableName(node) {
  if (!ts.isPropertyAccessExpression(node)) {
    return null;
  }
  if (!ts.isIdentifier(node.expression) || node.expression.text !== 'zql') {
    return null;
  }
  return node.name.text;
}

function txMutateCall(node) {
  if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression)) {
    return null;
  }
  const op = node.expression.name.text;
  const tableAccess = node.expression.expression;
  if (!ts.isPropertyAccessExpression(tableAccess)) {
    return null;
  }
  const mutateAccess = tableAccess.expression;
  if (
    !ts.isPropertyAccessExpression(mutateAccess)
    || mutateAccess.name.text !== 'mutate'
    || !ts.isIdentifier(mutateAccess.expression)
    || mutateAccess.expression.text !== 'tx'
  ) {
    return null;
  }
  return {table: tableAccess.name.text, op};
}

function findExportedObject(sourceFile, exportName, callName) {
  for (const statement of sourceFile.statements) {
    if (!ts.isVariableStatement(statement)) {
      continue;
    }
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name) || declaration.name.text !== exportName || !declaration.initializer) {
        continue;
      }
      const call = asCall(declaration.initializer, callName);
      const object = call?.arguments.find(ts.isObjectLiteralExpression);
      if (object) {
        return object;
      }
    }
  }
  return null;
}

function asCall(node, name) {
  return ts.isCallExpression(node) && expressionName(node.expression) === name ? node : null;
}

function buildPairs(queries, mutators) {
  const pairs = [];
  for (const query of queries) {
    const readTables = new Set(query.readTables);
    for (const mutator of mutators) {
      const tables = mutator.writeTables.filter(table => readTables.has(table));
      if (tables.length === 0) {
        continue;
      }
      pairs.push({
        queryName: query.queryName,
        mutatorName: mutator.mutatorName,
        overlappingTables: tables.sort(),
        queryLoc: query.loc,
        mutatorLoc: mutator.loc,
      });
    }
  }
  return pairs.sort((a, b) => `${a.queryName}:${a.mutatorName}`.localeCompare(`${b.queryName}:${b.mutatorName}`));
}

function buildQueryImpactGroups(queries, mutators, pairs) {
  const queryByName = new Map(queries.map(query => [query.queryName, query]));
  const mutatorByName = new Map(mutators.map(mutator => [mutator.mutatorName, mutator]));
  const grouped = new Map();
  for (const pair of pairs) {
    const query = queryByName.get(pair.queryName);
    const mutator = mutatorByName.get(pair.mutatorName);
    const group = grouped.get(pair.queryName) ?? {
      queryName: pair.queryName,
      queryLoc: pair.queryLoc,
      readTables: query?.readTables ?? [],
      mutators: [],
    };
    group.mutators.push({
      mutatorName: pair.mutatorName,
      mutatorLoc: pair.mutatorLoc,
      writeTables: mutator?.writeTables ?? [],
      overlappingTables: pair.overlappingTables,
    });
    grouped.set(pair.queryName, group);
  }
  return [...grouped.values()]
    .map(group => ({
      ...group,
      mutatorCount: group.mutators.length,
      overlappingTables: [...new Set(group.mutators.flatMap(mutator => mutator.overlappingTables))].sort(),
      mutators: group.mutators.sort((a, b) => a.mutatorName.localeCompare(b.mutatorName)),
    }))
    .sort((a, b) => a.queryName.localeCompare(b.queryName));
}

function summarizeTables(queries, mutators) {
  const byTable = new Map();
  for (const query of queries) {
    for (const table of query.readTables) {
      const entry = byTable.get(table) ?? {table, queries: [], mutators: []};
      entry.queries.push(query.queryName);
      byTable.set(table, entry);
    }
  }
  for (const mutator of mutators) {
    for (const table of mutator.writeTables) {
      const entry = byTable.get(table) ?? {table, queries: [], mutators: []};
      entry.mutators.push(mutator.mutatorName);
      byTable.set(table, entry);
    }
  }
  return [...byTable.values()]
    .map(entry => ({
      table: entry.table,
      queryCount: new Set(entry.queries).size,
      mutatorCount: new Set(entry.mutators).size,
      queries: [...new Set(entry.queries)].sort(),
      mutators: [...new Set(entry.mutators)].sort(),
    }))
    .sort((a, b) => a.table.localeCompare(b.table));
}

function mergeUsage(target, source) {
  for (const table of source.directReadTables) target.directReadTables.add(table);
  for (const table of source.relatedReadTables) target.relatedReadTables.add(table);
  for (const table of source.writeTables) target.writeTables.add(table);
  for (const op of source.mutateOps) {
    const key = `${op.table}:${op.op}`;
    const existing = target.mutateOpsByKey.get(key) ?? {...op, count: 0, samples: []};
    existing.count += op.count;
    existing.samples = [...existing.samples, ...op.samples].slice(0, 5);
    target.mutateOpsByKey.set(key, existing);
  }
  for (const ref of source.relationRefs) target.relationRefs.push(ref);
  for (const helper of source.helperRefs) target.helperRefs.add(helper);
  for (const warning of source.warnings) target.warnings.push(warning);
}

function dedupeRelationRefs(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = `${item.method}:${item.sourceTable}:${item.relationName}:${item.destTable}:${item.loc.line}:${item.loc.column}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(item);
  }
  return out;
}

function dedupeWarnings(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = `${item.type}:${item.message}:${item.loc?.line}:${item.loc?.column}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(item);
  }
  return out;
}

function findCall(node, predicate) {
  if (ts.isCallExpression(node) && predicate(node)) {
    return node;
  }
  let found = null;
  ts.forEachChild(node, child => {
    if (!found) {
      found = findCall(child, predicate);
    }
  });
  return found;
}

function findPropertyValue(node, propertyName) {
  if (ts.isObjectLiteralExpression(node)) {
    for (const property of node.properties) {
      if (ts.isPropertyAssignment(property) && propertyNameText(property.name) === propertyName) {
        return property.initializer;
      }
    }
  }
  let found = null;
  ts.forEachChild(node, child => {
    if (!found) {
      found = findPropertyValue(child, propertyName);
    }
  });
  return found;
}

function visit(node, callback) {
  callback(node);
  ts.forEachChild(node, child => visit(child, callback));
}

function expressionName(expression) {
  if (ts.isIdentifier(expression)) {
    return expression.text;
  }
  if (ts.isPropertyAccessExpression(expression)) {
    return expression.name.text;
  }
  return null;
}

function propertyNameText(name) {
  if (ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name)) {
    return name.text;
  }
  return null;
}

function identifierText(node) {
  return node && ts.isIdentifier(node) ? node.text : null;
}

function parameterName(parameter) {
  if (!parameter || !ts.isIdentifier(parameter.name)) {
    return null;
  }
  return parameter.name.text;
}

function isFunctionLike(node) {
  return Boolean(node && (ts.isArrowFunction(node) || ts.isFunctionExpression(node)));
}

function isStringLike(node) {
  return Boolean(node && (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)));
}

function sorted(set) {
  return [...set].sort();
}

function sortedUnion(...sets) {
  return [...new Set(sets.flatMap(set => [...set]))].sort();
}

function loc(sourceFile, node) {
  const start = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
  return {
    file: sourceFile.fileName,
    line: start.line + 1,
    column: start.character + 1,
  };
}

function relative(root, path) {
  return path.startsWith(root) ? path.slice(root.length + 1) : path;
}

function parseArgs(args) {
  const parsed = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const next = () => {
      if (i + 1 >= args.length) {
        throw new Error(`Missing value for ${arg}`);
      }
      return args[++i];
    };
    if (arg === '--repo') {
      parsed.repo = next();
    } else if (arg === '--out') {
      parsed.out = next();
    } else if (arg === '--help' || arg === '-h') {
      console.log('Usage: node ./src/analyze-impact.mjs --repo ../.. --out ./inventories/generated/query-mutator-impact.generated.json');
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return parsed;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err => {
    console.error(err.stack ?? err.message);
    process.exit(1);
  });
}
