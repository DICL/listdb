# ListDB: a Key-Value Store for Byte-addressable Persistent Memory 

## Introduction

ListDB is a key-value store for byte-addressable persistent memory, which consists of three novel techniques: 
(i) byte-addressable Index-Unified Logging that incrementally converts write-ahead logs into SkipLists, 
(ii) Braided SkipList, a simple NUMA-aware SkipList that effectively reduces the NUMA effects of NVMM, and 
(iii) Zipper Compaction, which moves down the LSM-tree levels without copying key-value objects, but by merging SkipLists in place without blocking concurrent reads. 
Using the three techniques, ListDB makes background compaction fast enough to resolve the infamous write stall problem and shows 1.6x and 25x higher write throughputs than PACTree and Intel Pmem-RocksDB, respectively.

## Publication

Wonbae Kim, Chanyeol Park, Dongui Kim, Hyeongjun Park, Young-ri Choi, Alan Sussman, Beomseok Nam,  
ListDB: Union of Write-Ahead Logs and Persistent SkipLists for Incremental Checkpointing on Persistent Memory Authors
16th USENIX Symposium on Operating Systems Design and Implementation (USENIX OSDI), CARLSBAD, CA, USA. July 11-13, 2022.

Paper link: https://www.usenix.org/conference/osdi22/presentation/kim

