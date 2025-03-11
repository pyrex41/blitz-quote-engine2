{pkgs}: {
  deps = [
    pkgs.rustc
    pkgs.cargo
    pkgs.taskflow
    pkgs.rapidfuzz-cpp
    pkgs.libxcrypt
    pkgs.python313
    pkgs.git-lfs
  ];
}
