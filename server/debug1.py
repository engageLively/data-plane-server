from main import create_app
from data_plane.server.data_plane_server import add_data_plane_table
from data_plane.dataplane.data_plane_table import RowTable
from data_plane.dataplane.data_plane_utils import DATA_PLANE_STRING, DATA_PLANE_NUMBER

schema = [{"name": "name", "type": DATA_PLANE_STRING}, {"name": "age", "type": DATA_PLANE_NUMBER}]
rows = [['Tim', 24], ['Jill', 23], ['jane', 45], ['Fred', 57]]

rows2 = [['John', 65], ['George', 85], ['Alfred', 76], ['Susie', 77]]

add_data_plane_table('unprotected', RowTable(schema, rows))
add_data_plane_table('protected', RowTable(schema, rows2), [{"variable": "test", "value": "foo"}])

app = create_app()

app.run()