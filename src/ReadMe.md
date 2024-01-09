This is the code base for pB-HTAP

The major concept is to achieve the single index with hybrid page design

The default setting is to build one B+-Tree for each table with hybrid pages, and a global hybrid buffer pool is to cache frequently accessed pages

One can directly modify the the source code for manipulating workloads
