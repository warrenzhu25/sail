# 04 · SQL Parser and Analyzer

> **You'll learn:** how a SQL string becomes a syntax tree, then a spec plan;
> how Sail's parser is generated from Rust types; which SQL is supported; and
> how to add new syntax.
>
> **Crates:** `sail-sql-parser` (lexer, AST, parser), `sail-sql-macro`
> (derive macros that generate the parser), `sail-sql-analyzer` (AST → spec
> plan)

---

## 1. What This Component Does

SQL reaches Sail from several places:

- `spark.sql("…")` from PySpark (through Spark Connect),
- SQL statements from Flight SQL clients,
- view definitions, which are stored as SQL text and re-parsed when a view is
  read,
- smaller snippets: a data type string (`"struct<a:int>"`), an expression string
  (`df.selectExpr("a + 1")`), a table name (`"db.t"`), or a date or interval
  literal.

This component turns that text into Sail's **spec plan** (see
[05 · The Spec Plan Model](05-spec-plan-and-config.md)) in three steps:

```
"SELECT region, count(*) FROM sales.orders WHERE amount > 100 GROUP BY region"
   │  1. Lexer        → tokens:  SELECT · region · , · count · ( · * · ) · FROM · …
   │  2. Parser       → AST:     Query { select: [...], from: [...], where: ..., group_by: [...] }
   │  3. Analyzer     → spec:    Aggregate(Filter(Read("sales.orders"), amount > 100), [region], [count(*)])
   ▼
spec::Plan::Query(...)
```

---

## 2. Why It's Built This Way

**Sail has its own Spark SQL parser.** Generic SQL parsers don't match Spark's
dialect closely enough. Spark has backtick identifiers, `LATERAL VIEW`,
`DISTRIBUTE BY`/`CLUSTER BY`/`SORT BY`, `INSERT OVERWRITE DIRECTORY`, typed
literals like `INTERVAL '1' DAY` and `X'0A'`, the `<=>` operator, `DIV`, and
many statement forms. Owning the parser lets Sail match Spark's grammar and
error behavior exactly.

**The grammar is written as Rust types.** Each AST node is an ordinary Rust
struct or enum, and a derive macro generates its parser:

- a **struct** means "these parts in this order",
- an **enum** means "one of these alternatives",
- `Option<T>` means "optional", and `Vec<T>` or `Sequence<T, Comma>` means
  "repeated" or "separated by commas".

So the AST *is* the grammar. Adding syntax usually means adding a field or a
variant, and the parser follows automatically. The same derive also produces a
**syntax descriptor** (useful for generating grammar documentation) and a
**text** form that turns the AST back into SQL.

**Parsing and meaning are separate steps.** The parser only checks shape. The
analyzer maps shape to meaning but still doesn't look anything up: no catalogs,
no function registry, no schemas. All of that happens later, during resolution
(see [07 · The Plan Resolver](07-plan-resolver.md)). Keeping these steps
separate makes each one simple, easy to test, and reusable. The Spark Connect
and Flight SQL front ends both call the same two functions.

**Errors that point at the problem.** The parser is built on **chumsky**, a
parser combinator library that tracks spans (where each token came from) and
labels (what kind of thing was expected). A syntax error can say "expected an
expression at line 3, column 17" instead of just "parse error".

---

## 3. How It Works

### 3.1 Keywords are generated at build time

`data/keywords.txt` lists about 360 Spark SQL keywords in alphabetical order
(ADD, AFTER, ALL, …). A build script reads the file and generates:

- a `Keyword` enum with one variant per keyword (`SELECT` → `Select`,
  `SYSTEM_TIME` → `SystemTime`),
- a fast lookup table from text to keyword,
- one small parser type per keyword, so the AST can write `pub select: Select`.

To add a keyword, add a line to the file in sorted order.

### 3.2 The lexer: text → tokens

The lexer splits the input into tokens and keeps the original text and position
of each one:

| Token | Examples | Notes |
|-------|----------|-------|
| Word | `SELECT`, `orders`, `123`, `1e5` | Records whether the word is a keyword. Numbers start as words and are interpreted later. |
| String | `'abc'`, `"abc"`, `` `col name` ``, `X'0A'`, `r'\d+'` | Records the quote style and any prefix. Escape sequences aren't processed yet. |
| Whitespace | spaces, tabs, newlines | Kept as tokens so spans stay exact, then skipped by the parser. |
| Comment | `-- …`, `/* … */` | Block comments can be nested. |
| Punctuation | `(`, `)`, `,`, `.`, `+`, `<`, `=`, … | Multi-character operators (`<=>`, `>>>`, `::`) are assembled by the parser. |

Three **parser options** control dialect details: whether a quote can be
escaped by doubling it (`'it''s'`), whether triple-quoted strings are allowed,
and whether double-quoted text is an identifier or a string. Sail uses Spark's
defaults.

### 3.3 The parser: tokens → AST

Most AST types use the derived parser. Some can't be derived and are written by
hand:

- **Terminals**: keywords, literals, operators, identifiers.
- **Recursive types**: a query contains subqueries, and an expression contains
  expressions. These are declared first and defined afterwards, which the
  macro's `dependency` attribute supports.
- **Operator precedence**: expressions use a Pratt parser, which applies binding
  strength explicitly.

**Operator precedence**, from tightest to loosest binding, matching Spark:

| Level | Operators |
|-------|-----------|
| Postfix | field access `a.b`, subscript `a[0]`, `::` cast |
| Unary | `+a`, `-a`, `~a` |
| 24 | `*`, `/`, `%`, `DIV` |
| 23 | `+`, `-`, `\|\|` (string concatenation) |
| 22 | `<<`, `>>`, `>>>` |
| 21 | `&` |
| 20 | `^` |
| 19 | `\|` |
| 18 | `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `!<`, `!>` |
| Postfix | `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN`, `[NOT] LIKE`, `RLIKE`, … |
| Prefix | `NOT`, `!` |
| 14 | `AND` |
| 13 | `OR` |

So `a + b * c > 10 AND NOT d OR e` parses as
`((a + (b * c)) > 10 AND (NOT d)) OR e`.

**What a query looks like in the AST.** A query has an optional `WITH` clause,
a body, and modifiers (`WINDOW`, `ORDER BY`, `SORT BY`, `CLUSTER BY`,
`DISTRIBUTE BY`, `LIMIT`, `OFFSET`). The body is either a single `SELECT` or a
set operation (`UNION`, `INTERSECT`, `EXCEPT`/`MINUS`, with `ALL` or
`DISTINCT`). A `FROM` item can be a table name, a subquery, `VALUES`, a nested
join, or a table function. Every Spark join type is supported: inner, cross,
left/right/full outer, left/right semi, left/right anti, natural, and `USING`.

**Statements.** About 45 statement kinds are recognized:

| Group | Statements |
|-------|------------|
| Queries | `SELECT …`, `WITH …`, `VALUES …`, `TABLE t` |
| Catalogs and databases | `USE`, `SET CATALOG`, `CREATE`/`ALTER`/`DROP DATABASE`, `SHOW DATABASES`, `SHOW CATALOGS` |
| Tables | `CREATE TABLE`, `CREATE TABLE … AS SELECT`, `REPLACE TABLE`, `ALTER TABLE`, `DROP TABLE`, `REFRESH TABLE`, `SHOW TABLES`, `SHOW TABLE EXTENDED`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, `DESCRIBE` |
| Views | `CREATE`/`ALTER`/`DROP VIEW`, `SHOW VIEWS` |
| Functions | `REFRESH FUNCTION`, `DROP FUNCTION`, `SHOW FUNCTIONS` |
| Writes | `INSERT INTO`, `INSERT OVERWRITE`, `INSERT … REPLACE WHERE`, `INSERT OVERWRITE DIRECTORY`, `MERGE INTO`, `UPDATE`, `DELETE`, `LOAD DATA` |
| Caching | `CACHE TABLE`, `UNCACHE TABLE`, `CLEAR CACHE` |
| Session | `SET key = value`, `SET TIME ZONE` |
| Other | `EXPLAIN`, `ANALYZE TABLE[S]`, `COMMENT ON …` |

### 3.4 The analyzer: AST → spec plan

The analyzer walks the AST and builds spec plan nodes. It's split by kind of
node:

- **Statements** decide between a query and a command. A query becomes
  `spec::Plan::Query`. `CREATE TABLE` becomes a create-table command node with a
  table definition (columns, format, partitioning, location, options), `USE db`
  becomes a set-current-database command, and so on.
- **Queries** become relational nodes: `FROM` becomes a read of a named table,
  `WHERE` a filter, the `SELECT` list a projection, `GROUP BY` an aggregate,
  `ORDER BY` a sort, and `LIMIT` a limit. `WITH` clauses become named
  subqueries. `LATERAL VIEW explode(…)` becomes a generator.
- **Expressions** become spec expressions. Names become *unresolved
  attributes*, function calls become *unresolved functions*, `*` becomes an
  *unresolved star*, and `CASE`, `CAST`, `EXTRACT`, `SUBSTRING`, lambdas,
  subqueries, and `EXISTS` each map to their spec form.
- **Literals** are parsed fully: numbers (integer, decimal, and float, including
  Spark suffixes like `10L`, `1.5BD`, `2D`), strings with escapes, binary
  (`X'…'`), dates, timestamps (with and without time zone), and intervals
  (`INTERVAL '1-2' YEAR TO MONTH`, `INTERVAL 3 DAYS 4 HOURS`).
- **Data types** map Spark type names (`INT`, `BIGINT`, `DECIMAL(10,2)`,
  `STRING`, `VARCHAR(10)`, `ARRAY<…>`, `MAP<…>`, `STRUCT<…>`, `TIMESTAMP_NTZ`, …)
  to spec types.

Nothing is looked up. A query that mentions a table that doesn't exist still
analyzes fine. The error comes later, during resolution.

### 3.5 Parameters

`spark.sql("SELECT * FROM t WHERE id = ?", args=[42])` and the named form
`:id` are supported. The Spark Connect layer converts the arguments into
literal or expression values and attaches them to the plan. Placeholders in the
SQL become *placeholder* expressions that the resolver fills in.

---

## 4. Examples

### 4.1 Walking through one query

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
ORDER BY n DESC
LIMIT 10
```

| Step | Result |
|------|--------|
| Tokens | `SELECT`(keyword) · `region`(word) · `,` · `count` · `(` · `*` · `)` · `AS`(keyword) · `n` · `FROM`(keyword) · `sales` · `.` · `orders` · `WHERE` · `amount` · `>` · `100` · … |
| AST | A query whose body is a select with two projection items, `FROM` as the table name `sales.orders`, `WHERE` as a binary expression (`amount` `>` `100`), and `GROUP BY` `region`. Its modifiers are `ORDER BY` `n DESC` and `LIMIT 10`. |
| Spec | Limit(10, Sort([n DESC], Aggregate(group=[region], agg=[region, count(*) AS n], Filter(amount > 100, Read(table "sales.orders"))))) |

Every name in the spec (`region`, `amount`, `n`, `count`, `sales.orders`) is
still unresolved.

### 4.2 Snippet parsing from DataFrames

```python
df.selectExpr("upper(name) AS u", "price * qty AS total")   # parse_expression
df.filter("amount > 100")                                    # parse_expression
spark.createDataFrame(rows, "id INT, tags ARRAY<STRING>")    # parse_data_type
spark.table("db.t")                                          # parse_object_name
```

### 4.3 Seeing the parse

`EXPLAIN` and the analyzer's tests print the AST and spec plans. Many tests
check a **round trip**: parse the SQL, turn it back into text, and compare it
with the expected normalized form.

---

## 5. End-to-End: `spark.sql("CREATE TABLE t (id INT) USING parquet")`

| # | What happens |
|---|--------------|
| 1 | Spark Connect receives a SQL command and calls `parse_one_statement`. |
| 2 | The lexer produces tokens: `CREATE`, `TABLE`, `t`, `(`, `id`, `INT`, `)`, `USING`, `parquet`. |
| 3 | The parser tries each statement form and matches *create table*: name `t`, a column list with `id` of type `INT`, and a `USING parquet` clause. |
| 4 | The analyzer turns this into a create-table command with a table definition: the column `id` with type Int32, format `parquet`, and no location (so it's a managed table). |
| 5 | Because it's a command, Spark Connect runs it immediately. The resolver turns it into a catalog command operator, the catalog creates the table, and an empty result is returned (see [03 · Spark Connect](03-spark-connect.md)). |

---

## 6. Extending the SQL Grammar

**To add a new clause or statement:**

1. Add any new keywords to `data/keywords.txt`, in sorted order.
2. Add fields or variants to the AST type in `src/ast/` (`statement.rs`,
   `query.rs`, or `expression.rs`). For example, a new statement is a new
   `Statement` variant whose fields are the keyword tokens and parts in order.
3. If the new part is recursive, set the `dependency` and `function` attributes
   so it receives the already-declared query or expression parser.
4. Map the new AST node to spec nodes in `sail-sql-analyzer`. If no spec node
   fits, add one (see [05](05-spec-plan-and-config.md)) and handle it in the
   resolver (see [07](07-plan-resolver.md)).
5. Add parser tests (round trip) and an end-to-end test. Gold-data tests in the
   repository compare results with real Spark (see
   [17 · Testing and Development](17-testing-and-development.md)).

**Pitfalls:**

- Derived parsers can't handle **left recursion**. Write such rules by hand
  (as with expression operators).
- Alternatives are tried **in order**. Put longer or more specific forms first,
  for example `LEFT SEMI` before `LEFT`.
- New keywords can break queries that use those words as identifiers. Check
  how Spark treats the keyword (reserved or not), and add tests that use it as
  a column name.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Keyword list and generator | `crates/sail-sql-parser/data/keywords.txt`, `crates/sail-sql-parser/build.rs` |
| Lexer and tokens | `crates/sail-sql-parser/src/lexer.rs`, `token.rs`, `string.rs` |
| AST (the grammar) | `crates/sail-sql-parser/src/ast/` (`statement.rs`, `query.rs`, `expression.rs`, `data_type.rs`, `literal.rs`, `identifier.rs`) |
| Parser options | `crates/sail-sql-parser/src/options.rs` |
| Derive macros (`TreeParser`, `TreeSyntax`, `TreeText`) | `crates/sail-sql-macro/src/` |
| Entry functions (`parse_one_statement`, `parse_expression`, …) | `crates/sail-sql-analyzer/src/parser.rs` |
| AST → spec | `crates/sail-sql-analyzer/src/` (`statement.rs`, `query.rs`, `expression.rs`, `data_type.rs`, `literal/`) |
| Callers | Spark Connect (`proto/plan.rs`), Flight SQL (`service.rs`), view expansion (`sail-plan/src/resolver/query/read.rs`) |
