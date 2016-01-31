import uuid
from cqlengine import columns
from cqlengine.models import Model

class ExampleModel(Model):
    read_repair_chance = 0.05 # optional - defaults to 0.1
    example_id      = columns.UUID(primary_key=True, default=uuid.uuid4)
    example_type    = columns.Integer(index=True)
    created_at      = columns.DateTime()
    description     = columns.Text(required=False)

#next, setup the connection to your cassandra server(s) and the default keyspace...
from cqlengine import connection
connection.setup(['127.0.0.1'], "cqlengine")

# or if you're still on cassandra 1.2
#connection.setup(['127.0.0.1'], "cqlengine", protocol_version=1)

# create your keyspace.  This is, in general, not what you want in production
# see https://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt for options
from cqlengine.management import create_keyspace
create_keyspace("cqlengine", "SimpleStrategy", 1)

#...and create your CQL table
from cqlengine.management import sync_table
sync_table(ExampleModel)

#now we can create some rows:
em1 = ExampleModel.create(example_type=0, description="example1", created_at=datetime.now())
em2 = ExampleModel.create(example_type=0, description="example2", created_at=datetime.now())
em3 = ExampleModel.create(example_type=0, description="example3", created_at=datetime.now())
em4 = ExampleModel.create(example_type=0, description="example4", created_at=datetime.now())
em5 = ExampleModel.create(example_type=1, description="example5", created_at=datetime.now())
em6 = ExampleModel.create(example_type=1, description="example6", created_at=datetime.now())
em7 = ExampleModel.create(example_type=1, description="example7", created_at=datetime.now())
em8 = ExampleModel.create(example_type=1, description="example8", created_at=datetime.now())

#and now we can run some queries against our table
print "Count of elements:", ExampleModel.objects.count()

