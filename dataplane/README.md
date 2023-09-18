# Basic Data Plane Data Structures
This directory contains the modules for the basic Data Plane data structures and a Python server framework.  The data plane structures are in the submodule dataplane, with the following three submodules:
1. data_plane_utils: this contains the constant type declarations and an exception thrown by the data plane routines
2.  conversion_utils.  The wire format for the data plane is JSON, with dates, times, and datetimes in ISO format.  These routines convert the wire format into the appropriate type for computation.  The routines also take default values to do a little lightweight data cleansing.
3. data_plane_table. This module contains the basic DataPlaneTable structure, which is the basis for both client- and server-side Data Plane operations.  It also contains the DataPlaneFilter class, which when instantiated filters DataPlaneTables.  In addition, a utility `check_valid_spec` checks to ensure that a filter specification is valid

