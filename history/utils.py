from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db import connections, models

from . import conf


def get_history_user_id(request):
    request_user_field = getattr(settings, 'HISTORY_REQUEST_USER_FIELD', 'user')
    user_attribute = getattr(settings, 'HISTORY_REQUEST_USER_ATTRIBUTE', 'pk')
    user = getattr(request, request_user_field)
    return getattr(user, user_attribute) if user_attribute else user


def create_history_table(user_id):
    params = {
        'table': getattr(settings, 'HISTORY_USER_TEMP_TABLE', 'history_user'),
        'field': getattr(settings, 'HISTORY_USER_FIELD', 'user_id'),
        'type': getattr(settings, 'HISTORY_USER_TYPE', 'integer'),
    }
    c = connections['default'].cursor()
    c.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS %(table)s (
            %(field)s %(type)s UNIQUE NOT NULL
        )
    """ % params)
    c.execute("TRUNCATE %(table)s" % params)
    c.execute("INSERT INTO %(table)s (%(field)s) VALUES (%%s)" % params, (user_id,))


def get_history_model(model_class):
    from history.management.commands.triggers import get_base_tables, truncate_long_name  # TODO: move this
    cursor = connections['default'].cursor()
    table_names = get_base_tables(cursor)
    pk_name, pk_type = table_names[model_class._meta.db_table]
    history_table = truncate_long_name(model_class._meta.db_table + '_history')
    pk_field = models.IntegerField if pk_type.startswith('int') else models.TextField
    value_field = JSONField if conf.USE_JSON else models.TextField
    user_field = models.IntegerField if conf.USER_TYPE.startswith('int') else models.TextField
    attributes = {
        '__module__': 'history',
        pk_name: pk_field(primary_key=True),
        'old_value': value_field(),
        'new_value': value_field(),
        'date_modified': models.DateTimeField(),
        conf.USER_FIELD: user_field(),
        'transaction_type': models.CharField(max_length=1),
    }
    if not conf.USE_JSON:
        attributes['field_name'] = models.CharField(max_length=64)
    type_name = '%sHistory' % model_class.__name__
    meta_name = '%sMeta' % model_class.__name__
    attributes['Meta'] = type(meta_name, (object,), {
        'db_table': '"%s"."%s"' % (conf.SCHEMA_NAME, history_table),
        'managed': False,
    })
    return type(type_name, (models.Model,), attributes)
