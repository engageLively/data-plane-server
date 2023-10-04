# data-plane-server

This is a reference server and client  for the Data Plane.  It demonstrates the Data Plane Server API.  It also functions as an open framework, so new Tables can be attached to the Data Plane server by providing a Class with a `get_rows()` method and a `columns` property.
The README.md file in each directory gives the documentation for the utilities and classes in that directory.  

The structure is as follows:
```
├── data_plane
│   ├── data_plane_server: A reference DataPlane server and middleware
│   ├── dataplane: The basic DataPlane types, including Filters and Tables
│   ├── examples: An example DataPlane server using the data_plane_server classes and utilities
│   ├── tests: pytest tests of the data plane
  
# The Simple Data Transfer Protocol

The Data Plane Server and Data Plane Client implement the Simple Data Transfer Protocol, a universal way to query and transmit tabular data.  The SDTP uses http/https as the underlying transport protocol.

This is a quick summary of the Simple Data Transfer Protocol.  A more extended description can be found at:


## Basic Data Structure
The core data structure of the SDTP is a _table_, which is simply a list of list of values. Conceptually, it is equivalent to a SQL database table; each column has a specific type and each row is of the same, fixed length.

## Data Plane Data Types.
