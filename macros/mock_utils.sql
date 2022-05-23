{% macro parse_sql_with_transformations(input_str, options={}) %}
  {% set ns = namespace(prev_symbol = '', terms = [], current_term = '', prev_is_escape = false, prev_is_whitespace = false, active_string_term = false, num_of_opened_brackets = 0, got_first_term = false, prev_is_dash = false, prev_is_asterisk = false, is_comment = false, comment_type = '', prev_is_slash = false) %}
  {% set input_str = (input_str + ' ') %}
  {% for s in input_str %}
   {% if not ns.is_comment %}
    {% if not ns.active_string_term %}
      {% if s in [' ', '\t', '\n', '\r', ','] and ns.num_of_opened_brackets == 0 %}
        {% if not ns.prev_is_whitespace and ns.got_first_term %}
          {% set ns.terms = ns.terms + [ns.current_term] %}
          {% set ns.current_term = '' %}
          {% do log("DBT-SN reset current_term") %}
        {% endif %}
        {% set ns.prev_is_whitespace = true %}
      {% else %}
        {% set ns.got_first_term = true %}
        {% set ns.current_term = (ns.current_term + s) %}
        {% set ns.prev_is_whitespace = false %}
      {% endif %}
      {% if s == '(' %}
        {% set ns.num_of_opened_brackets = (ns.num_of_opened_brackets + 1) %}
      {% endif %}
      {% if s == ')' %}
        {% set ns.num_of_opened_brackets = (ns.num_of_opened_brackets - 1) %}
      {% endif %}
      {% if s == '-' and ns.prev_is_dash %}
        {% set ns.is_comment = true %}
        {% set ns.comment_type = 'single_line' %}
      {% endif %}
      {% if s == '*' and ns.prev_is_slash %}
        {% set ns.is_comment = true %}
        {% set ns.comment_type = 'multi_line' %}
      {% endif %}
    {% else %}
      {% set ns.current_term = (ns.current_term + s) %}
    {% endif %}
    {% if s == "'" and not ns.prev_is_escape %}
      {% set ns.active_string_term = not ns.active_string_term %}
    {% endif %}
   {% else %}
      {% if s in ['\r', '\n'] and ns.comment_type == 'single_line' %}
        {% set ns.is_comment = false %}
        {% if ns.current_term | length >= 2 %}
          {% set ns.current_term = ns.current_term[:-2] %}
        {% endif %}
      {% endif %}
      {% if s == '/' and ns.prev_is_asterisk and ns.comment_type == 'multi_line' %}
        {% set ns.is_comment = false %}
        {% if ns.current_term | length >= 2 %}
          {% set ns.current_term = ns.current_term[:-2] %}
        {% endif %}
      {% endif %}
   {% endif %}
   {% set ns.prev_is_escape = (s == "'") %}
   {% set ns.prev_is_dash = (s == "-") %}
   {% set ns.prev_is_slash = (s == "/") %}
   {% set ns.prev_is_asterisk = (s == "*") %}
    {% do log("DBT Unit Testing parse SQL, symbol = '" ~ s ~ "', prev = '" ~ ns.prev_symbol ~ "', prev is escape = " ~ ns.prev_is_escape ~ ", prev is whitespace = " ~ ns.prev_is_whitespace ~ ", active_string_term = " ~ ns.active_string_term ~ ", current_term = '" ~ ns.current_term ~ "', num_of_opened_brackets = " ~ ns.num_of_opened_brackets ~ ", got_first_term = " ~ ns.got_first_term ~ ", prev_dash = " ~ ns.prev_is_dash ~ ", is comment = " ~ ns.is_comment ~ ", terms = '" ~ ns.terms ~ "'") %}
    {% set ns.prev_symbol = s %}
  {% endfor %}
  {% set nullish_columns = options.get("nullish_columns", '').split(',') | map('trim') | reject('==', '') | list %}
  {% set rs = namespace(rows = [], current_row_index = -1, next_is_key = false, next_value = '', current_row = {}) %}
  {% for nc in nullish_columns %}
    {% do rs.current_row.update({nc: "null"}) %}
  {% endfor %}
  {% for t in ns.terms %}
    {% set lt = t | lower %}
    {% if lt not in ['union', 'all', 'as'] %}
      {% if lt == 'select' %}
        {% set rs.current_row_index = (rs.current_row_index + 1) %}
        {% if rs.current_row_index > 0 %}
          {% set rs.rows = rs.rows + [rs.current_row] %}
          {% set rs.current_row = {} %}
          {% for nc in nullish_columns %}
            {% do rs.current_row.update({nc: "null"}) %}
          {% endfor %}
        {% endif %}
      {% elif lt == '%%previous_row%%' %}
        {% do rs.current_row.update(rs.rows[rs.current_row_index - 1]) %}
      {% elif lt in ['array', 'cast', 'timestamp', 'interval', 'time', 'date', 'map', 'uuid'] %}
        {% set rs.next_value = lt %}
      {% else %}
        {% if rs.next_is_key %}
          {% do rs.current_row.update({t: rs.next_value}) %}
          {% set rs.next_value = '' %}
        {% else %}
          {% set rs.next_value = rs.next_value + t %}
        {% endif %}
        {% set rs.next_is_key = not rs.next_is_key %}
      {% endif %}
      {% do log("DBT Unit Testing parse TERMS, term lower = " ~ lt ~ ", next_value = " ~ rs.next_value ~ ", next is key = " ~ rs.next_is_key ~ "', cur row index = '" ~ rs.current_row_index ~ "', cur row = " ~ rs.current_row ~ ", rows = " ~ rs.rows) %}
    {% endif %}
    {% if loop.last %}
      {% set rs.rows = rs.rows + [rs.current_row] %}
    {% endif %}
  {% endfor %}
  {% do log("DBT Unit Testing Terms parsed rows = " ~ rs.rows ~ ", nullish columns = " ~ nullish_columns) %}
  {% set fs = namespace(final_sql = '') %}
  {% for r in rs.rows %}
    {% set columns = r.keys() | sort %}
    {% if fs.final_sql | length == 0 %} 
      {% set fs.final_sql = "          SELECT " %}
    {% else %}
      {% set fs.final_sql = fs.final_sql + "\nUNION ALL SELECT " %}
    {% endif %}
    {% for col in columns %}
      {% set fs.final_sql = fs.final_sql + r[col] + ' AS ' + col %}
      {% if not loop.last %}
        {% set fs.final_sql = fs.final_sql + ", " %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  {# do log("DBT Unit Testing Parser final SQL = \n" ~ fs.final_sql) #}
  {{ return (fs.final_sql) }}
{% endmacro %}


{% macro sql_from_csv(options={}) %}
  {{ return (sql_from_csv_input(caller(), options)) }}
{% endmacro %}

{% macro sql_from_csv_input(csv_table, options={}) %}
  {% set unit_tests_config = var("unit_tests_config", {}) %}
  {% set column_separator = options.get("column_separator", unit_tests_config.get("column_separator", ",")) %}
  {% set line_separator = options.get("line_separator", unit_tests_config.get("line_separator", "\n")) %}
  {% set type_separator = options.get("type_separator", unit_tests_config.get("type_separator", "::")) %}
  {% set quote_symbol_to_wrap_values = options.get("quote_symbol_to_wrap_values", unit_tests_config.get("quote_symbol_to_wrap_values", "")) %}
  {% set symbol_to_unwrap_values = options.get("symbol_to_unwrap_values", unit_tests_config.get("symbol_to_unwrap_values", "")) %}
  {% set types_to_not_wrap = options.get("types_to_not_wrap", unit_tests_config.get("types_to_not_wrap", [])) %}
  {% set ns = namespace(col_names=[], col_types = [], col_values = [], row_values=[]) %}

  {% set rows = csv_table.split(line_separator) | map('trim') | reject('==', '') | list %}
  {% set cols = rows[0].split(column_separator) | map('trim') %}
  {% for col in cols %}
    {% set c = col.split(type_separator) | list %}
    {% set col_name = c[0] %}
    {% set col_type = c[1] %}
    {% set ns.col_names = ns.col_names + [col_name] %}
    {% set ns.col_types = ns.col_types + [col_type] %}
  {% endfor %}

  {% for row in rows[1:] %}
    {% set cols = row.split(column_separator) | map('trim') | list %}
    {% set ns.col_values = [] %}
    {% for col in cols %}
      {% set col_value_unwrapped = col %}
      {% if symbol_to_unwrap_values | length > 0 and col[0] == symbol_to_unwrap_values and col[-1] == symbol_to_unwrap_values %}
        {% set col_value_unwrapped = col | replace(symbol_to_unwrap_values, '', 0) %}
      {% endif %}
      {% set col_value = quote_symbol_to_wrap_values ~ col_value_unwrapped ~ quote_symbol_to_wrap_values %}
      {% set col_type = ns.col_types[loop.index-1] %}
      {% if col_type is defined %}
        {% if col_type in types_to_not_wrap %}
          {% set col_value = col_value_unwrapped %}
        {% endif %}
        {% set col_value = "CAST(" ~ col_value ~ " as " ~ col_type ~ ")" %}
      {% endif %}
      {% set col_value = col_value ~ " as " ~ ns.col_names[loop.index-1] %}
      {% set ns.col_values = ns.col_values + [col_value] %}
    {% endfor %}

    {% set col_values = ns.col_values | join(",") %}
    {% set sql_row = "select " ~ col_values %}
    {% set ns.row_values = ns.row_values + [sql_row] %}
  {% endfor %}

  {% set sql = ns.row_values | join("\n union all\n") %}

  {{ return (sql) }}

 {% endmacro %}

