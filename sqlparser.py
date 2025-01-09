import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, Where, Comparison, Parenthesis, Function
from sqlparse.tokens import Keyword, DML, Wildcard, Name
import json


def extract_columns_from_query(sql_query):
    parsed = sqlparse.parse(sql_query)
    columns = []
    table_aliases = {}
    join_conditions = []
    filter_conditions = []

    for statement in parsed:
        if not statement.get_type() == 'SELECT':
            continue

        # Step 1: Extract Table Names, Aliases, and Handle Nested Queries
        table_aliases = extract_table_aliases(statement)

        # Step 2: Extract Join Conditions
        join_conditions = extract_join_conditions(statement)

        # Step 3: Extract Filter Conditions
        filter_conditions = extract_filter_conditions(statement)

        # Step 4: Extract Columns
        for token in statement.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.extend(parse_column(identifier, table_aliases, join_conditions, filter_conditions))
            elif isinstance(token, Identifier):
                columns.extend(parse_column(token, table_aliases, join_conditions, filter_conditions))
            elif isinstance(token, Function):
                columns.extend(parse_function(token, table_aliases, join_conditions, filter_conditions))
            elif token.ttype is Wildcard:
                columns.append({
                    "name": "*",
                    "alias": None,
                    "section": "select",
                    "text": token.value,
                    "table": next(iter(table_aliases.values()), None)
                })

    return json.dumps(columns, indent=4)


def extract_table_aliases(statement):
    """Extract table aliases, including handling nested queries."""
    table_aliases = {}
    from_seen = False

    for token in statement.tokens:
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
        if from_seen and isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                table_aliases.update(process_table_or_subquery(identifier))
        if from_seen and isinstance(token, Identifier):
            table_aliases.update(process_table_or_subquery(token))
        if from_seen and isinstance(token, Parenthesis):
            subquery = extract_subquery(token)
            if subquery:
                table_aliases.update(subquery)

    return table_aliases


def process_table_or_subquery(identifier):
    """Process a table or subquery."""
    table_aliases = {}
    if '(' in identifier.value:  # Likely a subquery
        subquery = extract_subquery(identifier)
        if subquery:
            table_aliases.update(subquery)
    else:
        table_alias = get_token_alias(identifier)
        table_aliases[table_alias] = extract_full_table_name(identifier)

    return table_aliases


def extract_subquery(parenthesis):
    """Extract subquery content and alias."""
    if not isinstance(parenthesis, Parenthesis):
        return {}

    subquery_content = parenthesis.value[1:-1]  # Remove enclosing parentheses
    subquery_alias = None
    for token in parenthesis.tokens:
        if isinstance(token, Identifier):
            subquery_alias = get_token_alias(token)
            break

    subquery_columns = extract_columns_from_query(subquery_content)
    return {subquery_alias: subquery_columns}


def extract_full_table_name(identifier):
    """Extract schema.table if schema exists, else just table."""
    schema = identifier.get_parent_name()
    table = identifier.get_real_name()
    if schema:
        return f"{schema}.{table}"
    return table


def extract_join_conditions(statement):
    """Extract join conditions from ON clauses."""
    join_conditions = []
    join_keywords = ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]

    for token in statement.tokens:
        if any(keyword in token.value.upper() for keyword in join_keywords):
            next_token = next((t for t in statement.tokens if isinstance(t, Comparison)), None)
            if next_token:
                join_conditions.append(next_token.value)
    return join_conditions


def extract_filter_conditions(statement):
    """Extract filter conditions from the WHERE clause."""
    filter_conditions = []

    for token in statement.tokens:
        if isinstance(token, Where):
            for subtoken in token.tokens:
                if isinstance(subtoken, Comparison):
                    filter_conditions.append(subtoken.value)
    return filter_conditions


def parse_column(identifier, table_aliases, join_conditions, filter_conditions):
    columns = []
    alias = get_token_alias(identifier)

    if '(' in identifier.value:  # Handle expressions like (c.lastName || ...)
        sub_identifiers = [token for token in identifier.tokens if isinstance(token, Identifier)]
        for sub_identifier in sub_identifiers:
            columns.append(build_column_metadata(sub_identifier, alias, identifier.value, table_aliases, join_conditions, filter_conditions))
    else:
        columns.append(build_column_metadata(identifier, alias, identifier.value, table_aliases, join_conditions, filter_conditions))

    return columns


def parse_function(function, table_aliases, join_conditions, filter_conditions):
    """Parse aggregation functions like SUM, COUNT, etc."""
    columns = []
    column_name = function.get_parameters()[0].get_real_name() if function.get_parameters() else None
    alias = get_token_alias(function)
    table_alias = function.get_parent_name()
    table = table_aliases.get(table_alias, table_alias)

    columns.append({
        "name": column_name,
        "alias": alias,
        "section": "select",
        "text": function.value,
        "table": table,
        "isJoinCondition": False,
        "joinText": None,
        "isFilterCondition": False,
        "conditionText": None,
        "isAggregation": True
    })

    return columns


def build_column_metadata(identifier, alias, text, table_aliases, join_conditions, filter_conditions):
    table_alias = identifier.get_parent_name()
    table = table_aliases.get(table_alias, table_alias)
    column_name = identifier.get_real_name()

    is_join = any(column_name in cond for cond in join_conditions)
    join_text = next((cond for cond in join_conditions if column_name in cond), None)

    is_filter = any(column_name in cond for cond in filter_conditions)
    filter_text = next((cond for cond in filter_conditions if column_name in cond), None)

    return {
        "name": column_name,
        "alias": alias,
        "section": "select",
        "text": text,
        "table": table,
        "isJoinCondition": is_join,
        "joinText": join_text if is_join else None,
        "isFilterCondition": is_filter,
        "conditionText": filter_text if is_filter else None
    }


def get_token_alias(token):
    """Safely get alias from a token."""
    if isinstance(token, (Identifier, Function)):
        return token.get_alias()
    return None

