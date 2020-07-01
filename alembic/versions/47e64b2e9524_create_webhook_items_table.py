"""Create webhook items table

Revision ID: 47e64b2e9524
Revises: 
Create Date: 2020-05-13 22:51:09.387155-07:00

"""
from alembic import op
import sqlalchemy as sa
import sqlalchemy.schema as sas


# revision identifiers, used by Alembic.
revision = "47e64b2e9524"
down_revision = None
branch_labels = None
depends_on = None


# add locally needed definitions
# don't create or delete sequences unless the underlying database supports it
def create_seq(name):
    if op._proxy.migration_context.dialect.supports_sequences:
        op.execute(sas.CreateSequence(sas.Sequence(name)))


def delete_seq(name):
    if op._proxy.migration_context.dialect.supports_sequences:
        op.execute(sas.DropSequence(sas.Sequence(name)))


def upgrade():
    create_seq("an_form_items_id_seq")
    op.create_table(
        "an_form_items",
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
            server_default=sa.text("nextval('an_form_items_id_seq')"),
        ),
        sa.Column("form_name", sa.Text, nullable=False,),
        sa.Column("body", sa.JSON, nullable=False,),
    )


def downgrade():
    op.drop_table("an_form_items")
    delete_seq("an_form_items_id_seq")
