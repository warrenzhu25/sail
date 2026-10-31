# 04 · SQL Parser and Analyzer: From Text to a Plan

SQL reaches Sail from several directions: `spark.sql(...)` calls from PySpark,
statements from Flight SQL clients, and the definitions of saved views, which
are stored as SQL text and parsed again each time the view is read. Smaller
fragments arrive too, such as a type string like `"id INT, tags ARRAY<STRING>"`
in `createDataFrame`, an expression in `df.selectExpr("price * qty")`, or a
table name in `spark.table("db.t")`.

All of these go through the same three steps. A **lexer** cuts the text into
tokens, a **parser** arranges the tokens into a syntax tree, and an
**analyzer** turns the tree into Sail's spec plan. This doc explains each step,
why Sail wrote its own parser, and how to add new syntax.

```text
"SELECT region, count(*) FROM sales.orders WHERE amount > 100 GROUP BY region"
   │  lexer    → SELECT · region · , · count · ( · * · ) · FROM · sales · . · orders · WHERE · …
   │  parser   → a syntax tree: Query { select list, from, where, group by }
   │  analyzer → spec plan: Aggregate(Filter(Read("sales.orders"), amount > 100), group by region)
   ▼
spec plan (names still unresolved)
```

---

## Why Sail has its own Spark SQL parser

General-purpose SQL parsers don't match Spark's dialect closely enough. Spark
has backtick-quoted identifiers, `LATERAL VIEW`, `DISTRIBUTE BY` and `SORT BY`,
`INSERT OVERWRITE DIRECTORY`, typed literals such as `INTERVAL '1' DAY` and
`X'0A'`, the null-safe equality operator `<=>`, integer division with `DIV`, and
dozens of statement forms. Owning the parser lets Sail follow Spark's grammar
exactly and report errors the way Spark does.

The parser is built on chumsky, a Rust library for writing parsers out of small
composable pieces. It keeps track of where each token came from and what kind of
thing the parser expected at each point, which lets errors point at the exact
spot ("expected an expression at line 3, column 17") instead of just saying
"syntax error".

## The grammar is written as Rust types

The most unusual design decision is that Sail's SQL grammar isn't written as
grammar rules at all. It's written as ordinary Rust structs and enums, and a
macro generates the parser from them.

The idea is simple. A struct means "these parts, in this order". An enum means
"one of these alternatives". An optional field means "this part may be
missing", and a list field means "this part may repeat", optionally separated by
commas. So the definition of a query (an optional WITH clause, a body, then
modifiers like ORDER BY and LIMIT) is literally a struct with those three
fields. The syntax tree *is* the grammar.

This pays off when Spark adds syntax: supporting a new clause usually means
adding a field or an enum variant, and the parser follows automatically. The
same macro also generates a way to print the tree back as SQL, which the tests
use to check that parsing round-trips correctly, and a description of the syntax
that can be used to document the grammar.

Some parts of the grammar can't be generated this way and are written by hand.
Terminals such as keywords, literals, and operators are hand-written. Recursive
structures, like a query containing a subquery or an expression containing
expressions, are declared first and filled in afterwards. Operator precedence
gets special handling too, as described below.

## Step one: cutting text into tokens

Sail's keyword list is a plain text file of about 360 Spark keywords, one per
line, in alphabetical order. At build time, a script reads the file and
generates a keyword type, a fast lookup table, and one small parser per keyword.
Adding a keyword means adding a line to the file.

The lexer walks through the text and produces tokens, remembering the exact
original text and position of each one. A word might be a keyword, a name, or
the start of a number (numbers are interpreted later, so 1e5 and 10L both start
life as words). Strings remember which quote style they used, because in Spark a
backtick-quoted string is an identifier while a single-quoted one is a literal.
Escape sequences aren't processed yet. Whitespace and comments become tokens
too, so positions stay exact, and the parser skips them. Block comments can be
nested.

## Step two: arranging tokens into a tree

The parser tries the grammar's alternatives against the tokens. For statements,
that means trying each statement form until one fits. Sail recognizes about 45:
queries; USE and SET CATALOG; creating, altering, and dropping databases,
tables, and views; the SHOW and DESCRIBE family; INSERT INTO and INSERT
OVERWRITE (including the REPLACE WHERE and DIRECTORY forms); MERGE, UPDATE,
DELETE, and LOAD DATA; caching commands; SET for configuration and time zone;
EXPLAIN; ANALYZE TABLE; and COMMENT ON.

Inside queries, the parser handles everything Spark allows in a FROM clause:
table names, subqueries, VALUES, nested joins, and table functions, joined with
inner, cross, left, right, and full outer joins, semi and anti joins, natural
joins, and USING. Set operations (UNION, INTERSECT, and EXCEPT or MINUS, each
with ALL or DISTINCT) and the modifiers WINDOW, ORDER BY, SORT BY, CLUSTER BY,
DISTRIBUTE BY, LIMIT, and OFFSET are all part of the query structure.

### How operator precedence works

Expressions need more care than the rest of the grammar, because
`a + b * c > 10 AND NOT d OR e` has to group the way Spark groups it. Sail uses
a Pratt parser, which assigns each operator a binding strength. From tightest to
loosest, matching Spark: field access, subscripts, and `::` casts first; then
unary plus, minus, and bitwise not; then multiplication, division, modulo, and
DIV; then addition, subtraction, and string concatenation with `||`; then
bit shifts; then bitwise and, xor, and or; then all the comparisons; then
postfix tests like IS NULL, BETWEEN, IN, and LIKE; then NOT; then AND; and
finally OR.

So `a + b * c > 10 AND NOT d OR e` parses as
`((a + (b * c)) > 10 AND (NOT d)) OR e`, and `a = 1 OR b = 2 AND c = 3` means
`a = 1 OR (b = 2 AND c = 3)`. When in doubt, add parentheses; the parser keeps
them.

### What a syntax error looks like

A malformed statement fails here, before anything else runs. Because tokens
carry their positions and the parser knows what it expected, the error can point
at the problem:

```python
>>> spark.sql("SELECT * FORM t")
pyspark.errors.exceptions.connect.ParseException: ... (points at the unexpected token "FORM")
```

## Step three: turning the tree into a spec plan

The analyzer walks the syntax tree and builds spec plan nodes (see
[05](05-spec-plan-and-config.md)). At the top level it decides whether a
statement is a query or a command. A SELECT becomes a query plan. A CREATE TABLE
becomes a create-table command carrying the column list, format, partitioning,
location, and options. USE db becomes a set-current-database command, and so on.

Within a query, the analyzer maps clauses to relational steps: FROM becomes a
read of a named table, WHERE a filter, the SELECT list a projection, GROUP BY an
aggregation, ORDER BY a sort, LIMIT a limit. WITH clauses become named
subqueries, and LATERAL VIEW explode(...) becomes a generator step. Expressions
become spec expressions: names turn into unresolved column references, function
calls into unresolved function calls, and `*` into an unresolved star. CASE,
CAST, EXTRACT, SUBSTRING, lambdas, subqueries, and EXISTS each have their own
spec form.

Literals are interpreted fully at this stage, because their meaning doesn't
depend on the session. That covers numbers with Spark's suffixes (10L for a
long, 1.5BD for a decimal, 2D for a double), strings with escapes, binary
literals like X'0A', dates, timestamps with and without time zones, and
intervals in both styles: INTERVAL '1-2' YEAR TO MONTH and INTERVAL 3 DAYS 4
HOURS. Spark type names like DECIMAL(10,2), VARCHAR(10), and
ARRAY<STRUCT<...>> are turned into spec types.

### Nothing is looked up yet

The analyzer never consults a catalog, the function registry, or a schema. A
query that mentions a table that doesn't exist analyzes perfectly well:

```python
df = spark.sql("SELECT x FROM table_that_does_not_exist")   # no error yet
df.show()                                                   # now: TABLE_OR_VIEW_NOT_FOUND
```

The first line succeeds because the shape is valid SQL. The table is only looked
up when the plan is resolved (see [07](07-plan-resolver.md)). Keeping lookups out
of parsing and analysis makes both steps simple, deterministic, and easy to
test, and it puts every "doesn't exist" error in one place.

### Parameters

Parameterized queries, such as `spark.sql("SELECT * FROM t WHERE id = ?",
args=[42])` or the named form with `:id`, are supported. The placeholders become
placeholder expressions in the spec plan, the Spark Connect layer attaches the
argument values, and the resolver fills them in.

---

## Following one statement end to end

Take `spark.sql("CREATE TABLE t (id INT) USING parquet")`. Spark Connect receives
a SQL command and calls the parser's single-statement entry point. The lexer
produces the tokens CREATE, TABLE, t, (, id, INT, ), USING, and parquet. The
parser tries the statement forms and matches create-table, with the name t, a
column list holding id of type INT, and a USING clause. The analyzer turns this
into a create-table command with one Int32 column, format parquet, and no
location, which means a managed table. Because it's a command, Spark Connect runs
it immediately: the resolver wraps it in a catalog command, the catalog creates
the table, and an empty result goes back to the client.

## Adding new syntax

To support a new clause or statement, add any new keywords to the keyword file
in alphabetical order, then add a field or variant to the syntax tree types in
the parser crate. A new statement is usually a new variant whose fields are its
keywords and parts in order. If the new part contains queries or expressions,
use the macro's dependency attributes so it receives the already-declared
recursive parsers. Then teach the analyzer to turn the new tree node into spec
nodes, adding a spec node and resolver support if nothing existing fits. Finish
with a round-trip parser test and an end-to-end test; the gold data tests
compare results with real Spark (see [17](17-testing-and-development.md)).

Three pitfalls are worth knowing. Generated parsers can't handle left recursion,
so such rules must be written by hand, as the expression operators are.
Alternatives are tried in order, so longer and more specific forms (LEFT SEMI)
must come before shorter ones (LEFT). And a new keyword can break existing
queries that use that word as a column name, so check whether Spark treats it as
reserved, and test it as an identifier.

## Where to look in the code

The keyword list is crates/sail-sql-parser/data/keywords.txt, turned into code
by that crate's build.rs. The lexer and tokens are in src/lexer.rs, token.rs, and
string.rs, and the syntax tree, which doubles as the grammar, is under src/ast/
(statement.rs, query.rs, expression.rs, data_type.rs, literal.rs,
identifier.rs). Parser options are in src/options.rs. The derive macros that
generate parsers live in crates/sail-sql-macro. The analyzer is
crates/sail-sql-analyzer: parser.rs holds the entry points such as
parse_one_statement and parse_expression, and statement.rs, query.rs,
expression.rs, data_type.rs, and literal/ turn the tree into spec plans. Callers
include the Spark Connect translator, the Flight SQL service, and view expansion
in the resolver.
