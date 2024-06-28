# HaSiS
Code space for HaSiS, including the source code of pB-HTAP that implements the single index with hybrid pages for HTAP processing; and the benchmark tool that translates TPCC and TPCH workloads for evaluation.

tpcch-kv-bench.cc --> the benchmark tool to concurrently generate and issue TPCC and TPCH requests. You can change workloads here. Details can be found in benchmark.pdf

blpus_XX.cpp --> implementation for HaSiS, involving joint delta and PAX page (hybrid page), delta page allocation, background compactions, blind updates, etc.

thd_buffer_poo.cpp --> the implementation of global hybrid buffer pool




