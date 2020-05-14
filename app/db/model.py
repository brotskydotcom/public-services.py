# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import os

import sqlalchemy as sa


# this metadata is intended for use with a postgresql database
def db_get_url() -> str:
    url = os.getenv('DATABASE_URL', 'postgres:///public_services')
    # unfortunately the database package doesn't understand
    # that 'postgres:' (used by Heroku) is an alias of
    # 'postgresql:' and so won't connect; compensate for that.
    if url.startswith('postgres:'):
        url = 'postgresql:' + url[len('postgres:'):]
    return url


# export the metadata object
db_metadata = sa.MetaData()

# the table for AN webhooks
an_form_items_id_seq = sa.Sequence('an_form_items_id_seq', metadata=db_metadata)
an_form_items = sa.Table(
    'an_form_items',
    db_metadata,
    sa.Column('id', sa.Integer,
              primary_key=True,
              server_default=sa.text("nextval('an_form_items_id_seq')"),
              ),
    sa.Column('form_name', sa.Text,
              nullable=False,
              ),
    sa.Column('body', sa.JSON,
              nullable=False,
              ),
)
