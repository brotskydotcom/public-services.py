"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa
import sqlalchemy.schema as sas
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


# add locally needed definitions
# don't create or delete sequences unless the underlying database supports it
def create_seq(name):
    if op._proxy.migration_context.dialect.supports_sequences:
       op.execute(sas.CreateSequence(sas.Sequence(name)))


def delete_seq(name):
    if op._proxy.migration_context.dialect.supports_sequences:
       op.execute(sas.DropSequence(sas.Sequence(name)))


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
