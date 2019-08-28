# Design
## Requirements
- Concurrent hierarchical key-value supporting `uint32, uint64, float, double, string, blob` types capable of handling TBs of data
- `blob`s and `string`s could be arbitrary length
- CRUD ops
- Automatic key-expiration support
- x86/x64 support
- C++17
## Data structures
// todo: Elaborate on the B+tree DS choice
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.219.7269&rep=rep1&type=pdf
https://github.com/runshenzhu/palmtree
https://15721.courses.cs.cmu.edu/spring2019/papers/06-indexes/a16-graefe.pdf
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf
https://hyeontaek.com/papers/openbwtree-sigmod2018.pdf

// todo: beautify some stats

for Order 254 and 100M KVS, we have Node size of 3064 bytes, and Stats: 566591 Total, 562899 Leafs(99.3484%), i.e. (566591 - 562899) * 3064 / 1024 / 1024 ~= 11MB of pages. So we should definitely store them in memory?

100M keys:
1024 Order: leafs at level = 3
256 Order: max level at 4


// todo: page_cache/paged_file
https://15445.courses.cs.cmu.edu/fall2018/slides/05-bufferpool.pdf

## Page format
// todo

# Performance test results

## Syntetic `paged_file` vs single-threaded `ofstream` random write test
```
Writing ~10K pages of size 16KB, async has 8 threads:
Running test #0 ...
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
# Building
Currently the library was tested on Win10 + msvc v142 x64/x86 toolsets

  - `> git clone https://github.com/yuyoyuppe/ckvs/`
  - `> cd ckvs`
  - `> git submodule update --init --recursive`

Note: `<arch>` in the following lists could be `x86` or `x64`.
## Windows

  - [Get premake5](https://github.com/premake/premake-core/releases/download/v5.0.0-alpha14/premake-5.0.0-alpha14-windows.zip) and add it to your `PATH`
  - Open msvc developer prompt
  - `> vcpkg install boost:<arch>-windows`
  - `> premake5 vs2017` // no vs2019 support yet :<
  - `> cd build`
  - `> msbuild ckvs.sln /p:Configuration=Release /p:Platform=<arch> /p:PlatformToolset=v142`
  - `> cd bin_<arch>`


## Linux
  - [Get premake5](https://github.com/premake/premake-core/releases/download/v5.0.0-alpha14/premake-5.0.0-alpha14-linux.tar.gz) and add it to your `PATH`
  - install boost(aptitude/pacman)
  - `> premake5 vs2017`
  - `> cd build`
  - `> msbuild project.sln /p:Configuration=Release /p:Platform=<arch>`
  - `> cd bin_<arch>`

Now you can run `property_tests` or `functional_tests` binaries.
