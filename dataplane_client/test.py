from dataplane_client.client import DataPlaneClient

client = DataPlaneClient("http://127.0.0.1:8080")

result = client.get_filtered_rows("electoral_college", {
    "operator": "IN_RANGE",
    "min_val": 1860,
    "max_val": 1900,
    "column": "Year"
}
)

print(result)
