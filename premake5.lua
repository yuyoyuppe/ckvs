workspace "ckvs"
configurations {"Debug", "Release"}
language "C++"
system "windows"
architecture "x86_64"
cppdialect "C++17"
startproject "tests"
preferredtoolarchitecture "x86_64"

paths = {
  ClangFormatExecutable = "S:\\VS2017\\Common7\\IDE\\VC\\vcpackages\\clang-format.exe",
  ckvs = "src/ckvs/",
  tests = "src/tests/",
  build = "build/",
  deps = {
    fmt = "deps/fmt/"
  },
  modules = 'src/',
}

settings = {
  source_extensions = {".cpp", ".h"}
}

location(paths.build)

newaction {
	trigger = "clean",
	description = "Clean all generated build artifacts",
	onStart = function()
    os.rmdir(paths.build)
  end
}

newaction {
  trigger = "format",
  description = "Apply .clang-format style to all source files",
  onStart = function()
    for _, src_ext in ipairs(settings.source_extensions) do
      for _, file in ipairs(os.matchfiles(paths.modules .. '**' .. src_ext)) do
        os.executef('%s -i -style=file -fallback-style=none %s', paths.ClangFormatExecutable, file)
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
  basedir (src_path)
  targetdir (paths.build .. "bin")
  objdir (paths.build .. "obj")
  files { src_path .. "**.cpp", src_path .. "**.cc", src_path .. "**.h" }
  linkoptions { "-IGNORE:4221" }
  filter "configurations:Debug"
    symbols "On"
    runtime "Debug"
    defines { "DEBUG" }
    targetsuffix "_d"
  filter "configurations:Release"
    warnings "Extra"
    symbols "Off"
    symbols "FastLink"
    defines { "NDEBUG" }
    runtime "Release"
    optimize "On"
end

project "ckvs"
  kind "StaticLib"
  defines
  {
    "_SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING",
    "FMT_HEADER_ONLY",
    "_CRT_SECURE_NO_WARNINGS",
  }
  make_common_project_conf(paths.ckvs)

  project "tests"
    kind "ConsoleApp"
    links { "ckvs" }
    defines {  }
    includedirs {paths.ckvs}
    make_common_project_conf(paths.tests)

