.. include:: ../README.rst
This is a reference server and client  for the Data Plane.  It demonstrates the Data Plane Server API.  It also functions as an open framework, so new Tables can be attached to the Data Plane server by providing a Class with a `get_rows()` method and a `columns` property.
The README.md file in each directory gives the documentation for the utilities and classes in that directory.  

The structure is as follows:
```
├── data_plane
│   ├── data_plane_server: A reference DataPlane server and middleware
│   ├── dataplane: The basic DataPlane types, including Filters and Tables
│   ├── examples: An example DataPlane server using the data_plane_server classes and utilities
│   ├── tests: pytest tests of the data plane