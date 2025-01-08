import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Token, Where, Comparison
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
        
        # Step 1: Extract Table Names and Aliases
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
    """Extract table aliases from the FROM and JOIN clauses."""
    table_aliases = {}
    from_seen = False
    
    for token in statement.tokens:
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
        if from_seen and isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                table_aliases[identifier.get_alias() or identifier.get_real_name()] = extract_full_table_name(identifier)
        if from_seen and isinstance(token, Identifier):
            table_aliases[token.get_alias() or token.get_real_name()] = extract_full_table_name(token)
        if token.ttype is Keyword and 'JOIN' in token.value.upper():
            from_seen = True
    
    return table_aliases


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
    for token in statement.tokens:
        if token.ttype is Keyword and 'JOIN' in token.value.upper():
            next_token = next((t for t in statement.tokens if isinstance(t, Comparison)), None)
            if next_token:
                join_conditions.append(next_token.value)
    return join_conditions


def extract_filter_conditions(statement):
    """Extract filter conditions from the WHERE clause."""
    filter_conditions = []
    where_seen = False
    
    for token in statement.tokens:
        if isinstance(token, Where):
            where_seen = True
            for subtoken in token.tokens:
                if isinstance(subtoken, Comparison):
                    filter_conditions.append(subtoken.value)
    return filter_conditions


def parse_column(identifier, table_aliases, join_conditions, filter_conditions):
    columns = []
    alias = None
    
    if 'AS' in identifier.value.upper():
        alias = identifier.get_alias()
    
    if '(' in identifier.value:  # Handle expressions like (c.lastName || ...)
        sub_identifiers = [token for token in identifier.tokens if isinstance(token, Identifier)]
        for sub_identifier in sub_identifiers:
            columns.append(build_column_metadata(sub_identifier, alias, identifier.value, table_aliases, join_conditions, filter_conditions))
    else:
        columns.append(build_column_metadata(identifier, alias, identifier.value, table_aliases, join_conditions, filter_conditions))
    
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


# Example Query
query = """
select 
 c.id,
 c.dob as DOB,	
 (c.lastName || ', ' || c.firstName || ' '+substr(c.middleName, 1, 1)) as fullName
from db1.customer c
inner join db2.transaction t
on c.id == t.cid
where c.dob < '1980-01-01';
"""

# Extract and Print Columns
output = extract_columns_from_query(query)
print(output)
