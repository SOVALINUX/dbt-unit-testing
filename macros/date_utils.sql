{% macro n_days_ago(days=0, now_posixtime=0) %}
  {%- if now_posixtime == 0 -%}
    {{ return (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(days)) }}
  {%- else -%}
    {{ return (modules.datetime.datetime.utcfromtimestamp(now_posixtime) - modules.datetime.timedelta(days)) }}
  {%- endif -%}
{% endmacro %}

{% macro to_epoch(dt) %}
  {{ return ((dt - modules.datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000) }}
{% endmacro %}

{% macro to_posix(dt) %}
  {{ return (dt - modules.datetime.datetime.utcfromtimestamp(0)).total_seconds() }}
{% endmacro %}

{% macro to_iso(dt, sep=' ', timespec='milliseconds') %}
  {{ return (dt.isoformat(sep, timespec)) }}
{% endmacro %}

{% macro generate_n_days_ago_variables(now_posixtime=0) %}
  {% set result = {} %}
  {% for ind in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 50, 100, 200, 300, 500, 1000] %}
    {% do result.update({'-' ~ ind ~ 'd_dt': dbt_unit_testing.n_days_ago(days=ind, now_posixtime=now_posixtime)}) %}
    {% do result.update({'-' ~ ind ~ 'd_date': result['-' ~ ind ~ 'd_dt'].strftime('%Y-%m-%d')}) %}
    {% do result.update({'-' ~ ind ~ 'd_epoch': dbt_unit_testing.to_epoch(result['-' ~ ind ~ 'd_dt'])}) %}
    {% do result.update({'-' ~ ind ~ 'd_posix': dbt_unit_testing.to_posix(result['-' ~ ind ~ 'd_dt'])}) %}
    {% do result.update({'-' ~ ind ~ 'd_micros': (dbt_unit_testing.to_epoch(result['-' ~ ind ~ 'd_dt']) | int) * 1000}) %}
    {% do result.update({'-' ~ ind ~ 'd_micros_from_date': (dbt_unit_testing.to_epoch(result['-' ~ ind ~ 'd_dt'].replace(hour=0, minute=0, second=0, microsecond=0)) | int) * 1000}) %}
  {% endfor %}
  {{ return ( result ) }}
{% endmacro %}