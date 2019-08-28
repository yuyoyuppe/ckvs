workspace "ckvs"
configurations {"Debug", "Release"}
platforms {"x86", "x64"}
language "C++"
system "windows"

cppdialect "C++17"
startproject "property_tests"
preferredtoolarchitecture "x86_64"

paths = {
  ckvs = "src/ckvs/",
  property_tests = "src/property_tests/",
  perf_tests = "src/perf_tests/",
  build = "build/",
  deps = {
    llfio = "deps/llfio/include/"
  },
}

settings = {
  source_extensions = {".cpp", ".hpp"}
}

location(paths.build)

newaction {
	trigger = "clean",
	description = "Clean all generated build artifacts",
	onStart = function()
    os.rmdir(paths.build)
  end
}

newoption {
  trigger     = "clang-format-path",
  description = "Absolute path to the clang-format.exe. Used by the format action.",
  default = "S:\\VS2019\\VC\\Tools\\Llvm\\8.0.0\\bin\\clang-format.exe",

}
newaction {
  trigger = "format",
  description = "Apply .clang-format style to all source files",
  onStart = function()
    for _, src_ext in ipairs(settings.source_extensions) do
      for _, file in ipairs(os.matchfiles(paths.modules .. '**' .. src_ext)) do
        print('Formatting ' .. file)
        os.executef('%s -i -style=file -fallback-style=none %s', _OPTIONS['clang-format-path'], file)
      end
    end
  end
}

local function make_common_project_conf(src_path, use_pch)
  if use_pch then
    pchheader "pch.h"
    pchsource (src_path .. "pch.cpp")
  end
  flags { "FatalWarnings", "MultiProcessorCompile" }
  includedirs{src_path, paths.build}
  for _, path in pairs(paths.deps) do
    includedirs(path)
  end
  basedir (src_path)
  targetdir (paths.build .. "bin_%{cfg.architecture}")
  objdir (paths.build .. "obj")
  for _, src_ext in ipairs(settings.source_extensions) do
    files { src_path .. "**" .. src_ext}
  end
  linkoptions { "-IGNORE:4221" }
  filter "platforms:x86"
    architecture "x86"
  filter "platforms:x64"
    architecture "x86_64"
  filter "configurations:Debug"
    symbols "On"
    runtime "Debug"
    defines 
    { 
      "DEBUG",
      -- "VERBOSE_TEST"
    }
    targetsuffix "_d"
  filter "configurations:Release"
    warnings "Extra"
    symbols "Off"
    symbols "FastLink"
    defines
    {
      "NDEBUG",
      "BOOST_DISABLE_ASSERTS",
    }
    runtime "Release"
    optimize "On"
  
  filter {}
    defines
    {
      "ASSERTS",
      -- todo: move to windows only
      "CHECK_LEAKS",
      "_ENABLE_EXTENDED_ALIGNED_STORAGE", -- fix msvc bug 
      "_CRT_SECURE_NO_WARNINGS", -- we're using stdio.h only in tests, really!
      "_SILENCE_ALL_CXX17_DEPRECATION_WARNINGS", -- llfio
    }
    disablewarnings 
    { 
      "4324", -- msvc likes to warn about the padding being present...
    } 

end

project "ckvs"
  kind "StaticLib"
  defines
  {
    "_SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING",
  }
  make_common_project_conf(paths.ckvs)

  project "property_tests"
    kind "ConsoleApp"
    links { "ckvs" }
    defines 
    { 
      -- 'ENABLE_SSD_TEST',
      'SSD_PAGED_FILE_PATH="C:\\\\test.ckvs"',
      -- 'ENABLE_HDD_TEST',
      'HDD_PAGED_FILE_PATH="D:\\\\test.ckvs"',
      'ENABLE_RAM_TEST', -- ram drive
      'RAM_PAGED_FILE_PATH="R:\\\\test.ckvs"', 
    }
    includedirs {paths.ckvs}
    make_common_project_conf(paths.property_tests)

