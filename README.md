# Building
Currently the library was tested on Win10 + msvc v142 x64/x86 toolsets

  - `$ git clone https://github.com/yuyoyuppe/ckvs/`
  - `$ cd ckvs`
  - `$ git submodule update --init --recursive`

Note: `<arch>` in the following lists could be `x86` or `x64`.
## Windows

  - [Get premake5](https://github.com/premake/premake-core/releases/download/v5.0.0-alpha14/premake-5.0.0-alpha14-windows.zip) and add it to your `PATH`
  - Open msvc developer prompt
  - `$ vcpkg install boost:<arch>-windows`
  - `$ premake5 vs2017` // no vs2019 support yet :<
  - `$ cd build`
  - `$ msbuild ckvs.sln /p:Configuration=Release /p:Platform=<arch> /p:PlatformToolset=v142`
  - `$ cd bin_<arch>`


## Linux
  - [Get premake5](https://github.com/premake/premake-core/releases/download/v5.0.0-alpha14/premake-5.0.0-alpha14-linux.tar.gz) and add it to your `PATH`
  - install boost(aptitude/pacman)
  - `$ premake5 gmake2`
  - `$ cd build`
  - `$ make`
  - `$ cd bin_<arch>`


Now you can run `property_tests`. Please note that you can change tmp storage paths on different hdd-types.
# Design
## Requirements
- Concurrent hierarchical key-value supporting `uint32, uint64, float, double, string, blob` types capable of handling TBs of data
- `blob`s and `string`s could be arbitrary length
- CRUD operations on key-value pairs(currently insert/remove/find)
- Key-expiration support
- x86/x64 support
- C++17

## B+-tree
### Intro
B+-tree serves as an index, since it [gurantees](https://en.wikipedia.org/wiki/B-tree#Best_case_and_worst_case_heights) a very small number of random lookups on HDD. Therefore, even when the amount of keys is huge, e.g. for B+-tree of order 1024 with 100 million of int64 keys, only <1%(~3500) of nodes are non-leafs, meaning that we can always keep them in RAM with a tiny footprint of ~11MB. 
### Concurrency
[There's](https://ieeexplore.ieee.org/document/6544834) [been many](https://db.in.tum.de/~leis/papers/ART.pdf) [research papers](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf) on concurrent B*-like trees, and an excellent overview is [available here](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.219.7269&rep=rep1&type=pdf). Some of the newer lock-free variants require nontrivial amount of effort to implement and could yield non-optimal results for an on disk DBs, [see figures 13-14](https://hyeontaek.com/papers/openbwtree-sigmod2018.pdf). The classic choice is to use a certain [lock-coupling scheme](https://15721.courses.cs.cmu.edu/spring2019/papers/06-indexes/a16-graefe.pdf): perform the "crab-walk" node-locking while traversing the tree, relasing the safe nodes above. For example, to insert some value X into a tree of height 2:
```
                               Root A 
                              +-------------------------+
                              | 1. Write-lock A         |
                              | 2. select appr. child   |
                              +------+------------------+
       Internal B                    |
      +-------------------------+    |
      | 3. Write-lock B         +<---+
      | 4. if safe, unlock A    |
      +-----------------------+-+
                              |
                              v Leaf C
                           +--+----------------------+
                           | 5. Write-lock C         |
                           | 6. If safe, unlock A, B |
                           +-------------------------+


```
We can also optimistically assume that the node C won't be overflowed after insertion, and take
read-locks along the way. However, if our assumption was wrong, we would need to restart traversal from the root using write-locks once again.
### Implementation notes
 - Using an appropriate locking primitive is important, e.g. locking [boost::upgrade_mutex](https://www.boost.org/doc/libs/1_71_0/doc/html/thread/synchronization.html#thread.synchronization.mutex_types.upgrade_mutex) is ~10x slower than `std::shared_mutex` <sup>msvc v142, Win 10, i7-6700</sup>, but the latter requires additional support.
- Distributing children of two nodes without allocating additional Node of greater arity requires very careful index manipulation
- Recursive-along-the-height functions are ok, since the height is usually small enough to cause stack overflow
- Storing parent handles would possibly require updating thousands of nodes on rebalancing, so we need to trace them each time during root-to-leaf traversal
- Unionizing/templating node type instead of using virtual dispatch allows heavy inlining, zero copy-serialization and policy customizations
- To implement slotted values which could be loaded/saved without a (de)-serialization phase, we need a new concept - assign/compare proxy, which bptree could get from a locked_node_t interface. Then it performs all operations on _slots/_keys using that proxy. Those proxies will hold a slotted_storage pointer(to be able to transfer the actual data between pages) and kv-store pointer, so they can request an additional pages for overflow

## OS interaction
### IO
To minimize IO overhead, B-tree nodes and various metadata structures are all sized as multiples of [system page size](https://en.wikipedia.org/wiki/Page_(computer_memory)) and being manipulated asynchronously through an event-loop. While the memory mapped IO seems like an obvious first choice for the task, it lacks the explicit control over flushing(e.g. `PrefetchVirtualMemory` is only a suggestion, lacks `Evict` counterpart, Windows-only API etc.). Since the software knows exactly which pages should be flushed and loaded, it's reasonable to disable [OS file-caching](https://docs.microsoft.com/en-us/windows/win32/fileio/file-buffering) and use direct IO. Awesome [llfio](https://ned14.github.io/llfio/) library is used to abstract platform details like `OVERLAPPED` etc.

### Memory management, serialization
Strict memory budgeting and preallocating beforehand alleviates mostly all allocation problems at the cost of reduced flexibility. However, having tweakable at compile-time data structures allows generating required permutation set and hot-load it later.

To keep binary compatibility between stores on x86/x64, all arithmetic types are named explicitly, serialized structures do not contain any pointers and are carefully aligned. Endianness is also statically asserted. There's a peculiar issue of [type punning leading to undefined behavior in C++ UB](https://gist.github.com/shafik/848ae25ee209f698763cffee272a58f8), which could be [alleviated in](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0593r2.html) [various ways](https://ned14.github.io/quickcpplib/namespacequickcpplib_1_1__xxx_1_1detach__cast.html#abaa48201df2ef0251105559a11d05b38). Keeping the structures `is_trivially_copyable/constructible` and `reinterpret_cast`ing it from [standard-blessed type](http://en.cppreference.com/w/cpp/types/byte) _seems_ to be a way to go.

### Page cache
- Hash-table of fixed capacity and bucket-level spinlocking
- Page descriptors are linked-list to buckets, their lifetime is managed by lockfree stack
- Hybrid LRU/random replacement policty
- Need to be careful not to choose miniscule capacity to avoid livelocking(or callback `cleanup` from `page_lock`)

## Page formats
### Index Page

Always at 0 offset in the physical file. Pinned in the page cache. Could be partially mounted(referenced) to some *virtual store*.
```
magic sig, 4 bytes
   +      
   v      
+--+--------------------------------------+
| ckvs | name to page id b+-tree root map |
+-----------------------------------------+
             ^
             +
linearized tree of b+-tree roots, <rest of the page> bytes
handles folder structure, e.g.:
- parent_dir
|_ child_ckvs1 -> {tree_root pageID X}
|_ child_ckvs2 -> {tree root pageID Y}
|__|_grandchild_ckvs3 -> {tree root pageID Z}
...
```
Free Page Stack Page

When a new page is required or a cleaned one needs releasing, free page stack pages
come to an aid! They're located at fixed offsets in the file, specified by the `extend_granularity` setting.
```
number of free page IDs in this page, 4 bytes
   +      
   v      
+--+--------------------------------------------------+
|  N | free_page_id1, free_page_id2,... free_page_idN |
+----------------------------+------------------------+
                             ^
        free page ids array, <rest of the page> bytes
                 
```

### B+-tree Node Page
```
Leaf/Root/...,
1 byte             Either "inline" key/payload, or slot #id in the slotted storage. If a KP-pair
   |   # Keys,     has expiration, then payload is never inlined, order * 8 bytes each
   |   2 bytes     |       | 
   v       v       v       v
+--+-------+-------+-------+-----+
| Kind | nKeys | Keys | Payloads | <- serialized node<> struct
+--------------------------------+
        +                   /--------------------------slotted storage-----------------------\
        v                   |          2 bytes -v  2 bytes end_offset + 2 bytes size each    |
+-----------------------------------------------+-------------+--------V---------------------+
| B+-tree node |  metadata  | slots_end_offset | nUsed slots  | slot descS | slot values     |
+---------------------+--------------+------------------------+ ------------------+----------+
 8 bit metadata, -----^              ^               binary/string slot values ---^          
  - 2 bits for key type               |_ offset from the page end at which to prepend new slot
  - 2 bits for payload type              value, 2 bytes
  - 2 bits indicates key/payload overflow
  - 1 bit indicates expiration timestamp 
  - 1 bit reserved // todo: could be used to indicate removed state
  - possible types: Integer, Float, Double, Binary

- If a key's type != Binary -> stored inline
- If a payload's type != Binary && doesn't have expiration -> stored inline
- If a key/payload isn't stored inline, the highest bit indicates whether the value is Huge
  and the second highest bit indicates whether the value is overflown
  - If  Huge -> first 4 bytes is overflow_page_id, then 2 bytes for size in # of pages
  - If !Huge -> first 2 bytes is slot_id. next 4 bytes is PageId if overflown, or unused
- If payload has expiration, the first 4 bytes of its slot value is a timestamp.
```

### Overflow Page
Used when the string/binary is smaller than page_size, but can't fit in the embedded storage on the B+-tree Node Page. Created on demand, Node Page's mutators first check max #`overflow_tolerance` of existing overflow pages referenced from the embedded slotted storage, and allocate new one if couldn't get a slot.
```
+-----------------------------------------------------------------------------------------------+
| same slotted storage structure as on B+-tree Node Page, but with a capacity of an entire page |
+-----------------------------------------------------------------------------------------------+
```
### Huge Page

```
+---------------------------------------------------------------------------------------------+
| contiguous binary data, bulk-allocated from the Free Page Stack, so max size                |
|  of huge binary object = sizeof(page) * (extend_granularity - 1)                            |
+---------------------------------------------------------------------------------------------+
Note: First 4 bytes of the First Huge Page contain expiration timestamp as needed
```

### Expiration design 
-  minimal size overhead
-  we're *forced* to lazily remove expired values, need GC to compact 
- we could also remove expired on merge and set a corresponding flag in metadata

# Perf tests
- All times are wall clock time.
-  Unless specified otherwise, in all tests below B+-tree order = 1022, and page size = 16KB, all key/values are randomly shuffled `uint64_t`
- Compiled without `ASSERTS` define at solution scope
- Using DDR4 2133 RAMdisk. Peak witnessed throughput = ~600MB/s, typical = ~350MB/s
- Note that having 10M+ values in a single physical store seem to choke it at page_cache_capacity = 32768(should this be the case? investigate why - cache thrashing/bad page settings/too much memcpying etc.) and much earlier with an even smaller capacity

## Syntetic [`paged_file`](https://github.com/yuyoyuppe/ckvs/blob/9a0536ded0d3d2321c42afcc49a183b1f15d2fe8/src/property_tests/paged_file.cpp#L166) vs [single-threaded `ofstream`](https://github.com/yuyoyuppe/ckvs/blob/9a0536ded0d3d2321c42afcc49a183b1f15d2fe8/src/property_tests/paged_file.cpp#L30) random write test
```
Writing ~10K pages of size 16KB, async has 8 threads:
===HDD==
async completed in 1.976 sec
ofstream completed 33.468 sec
===SSD==
async completed in 1.206 sec
ofstream completed 0.784 sec
===RAM==
async completed in 1.133 sec
ofstream completed 0.541 sec
```

## Insert 10M, remove 5M, 8 threads, query another 5M, page_cache_capacity = 32768

Insert 10M of uint64 key/values, splitted randomly across 8 threads, concurrently removing odd keys, then save to file(in RAM), reopen it and find all even keys in 1 thread:
```
$ property_tests.exe
OK -- 14.174sec
```

## Remove 5M out of 5M, hot cache, page_cache_capacity = 32768
```
removed in 6.925 sec
```

## Remove 5M out of 5M, cold cache, page_cache_capacity = 32768
```
removed in 7.321 sec
```

## Query 1M from a cold cache, 8 threads, page_cache_capacity = 8192
```
Running test #0 ...
queried in 0.78 sec
OK -- 2.146sec
^ including single-threaded insert before the query threads are launched
```
## Query 1M from a cold cache, 8 threads, page_cache_capacity = 32768
```
queried in 0.74 sec
OK -- 2.514sec
```

## Query 5M from a cold cache, 8 threads, page_cache_capacity = 32768
```
queried in 4.951 sec
OK -- 13.751sec
```

## Query 10M from a cold cache, 8 threads, page_cache_capacity = 32768
```
$ property_tests.exe
queried in 9.953 sec
```

## Query 20M from a cold cache, 8 threads, page_cache_capacity = 8192
```
$ property_tests.exe
queried in 658.798 sec
OK -- 1152.19sec
```

## Query 20M from a cold cache, 8 threads, page_cache_capacity = 32768
```
$ property_tests.exe
Running test #0 ...
queried in 80.643 sec
OK -- 121.129sec
```

>todo: more? mixed load tests? 80/20 bigger cache size(relax power of 2 page_cache capacity restriction)?

