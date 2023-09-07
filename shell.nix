with import <nixpkgs> {};
stdenv.mkDerivation {
  name = "my-python-project";
  buildInputs = [ python3Packages.pillow python3Packages.shapely python3Packages.tqdm python3Packages.aiohttp ];
}