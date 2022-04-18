{% macro n_days_ago(days=0) %}
  {{ return (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(days)) }}
{% endmacro %}

{% macro to_epoch(dt) %}
  {{ return ((dt - modules.datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000) }}
{% endmacro %}

{% macro to_iso(dt, sep=' ', timespec='milliseconds') %}
  {{ return (dt.isoformat(sep, timespec)) }}
{% endmacro %}