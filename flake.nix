{
  description = "Jetstream development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    nixpkgs-go.url = "github:NixOS/nixpkgs/nixos-26.05";
    systems.url = "github:nix-systems/default";
  };

  outputs = { self, nixpkgs, nixpkgs-go, systems }:
    let
      eachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      devShells = eachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          goPkgs = import nixpkgs-go { inherit system; };
          goVersion = "1.27.3";
          # The pinned Nix channel can lag Go security patch releases.
          go = let
            patchedGo = goPkgs.go_1_26.overrideAttrs (previous: {
              version = goVersion;
              src = goPkgs.fetchurl {
                url = "https://dl.google.com/go/go${goVersion}.src.tar.gz";
                hash = "sha256-bndJA1zzYPBrJXXRYup535/ajvZxiJ5cN1+OUHd/Jrs=";
              };
              # Go 1.27 renamed the loader used by the vendoring patch.
              patches = builtins.filter
                (patch: baseNameOf (toString patch) != "go_no_vendor_checks-1.26.patch")
                previous.patches ++ [
                  (goPkgs.fetchurl {
                    url = "https://raw.githubusercontent.com/NixOS/nixpkgs/b80ce62850a74b8d5c1428e675a33862fc471373/pkgs/development/compilers/go/go_no_vendor_checks-1.27.patch";
                    hash = "sha256-aTpc6kAX9bAyMMAHqzldcb2EEseCpke7QlJuQ1Bk6jc=";
                  })
                ];
            });
          in
          assert pkgs.lib.assertMsg (patchedGo.version == goVersion)
            "nixpkgs-go must provide Go ${goVersion}, got ${patchedGo.version}";
          patchedGo;
          buildGoModule = pkgs.buildGoModule.override { inherit go; };
          golangci-lint = buildGoModule {
            pname = "golangci-lint";
            version = "2.15.0";
            src = pkgs.fetchFromGitHub {
              owner = "golangci";
              repo = "golangci-lint";
              rev = "v2.15.0";
              hash = "sha256-m9PoJHescRuT9DiKsb6LL+lQlc7RKAWDiRyf8pNMffA=";
            };
            vendorHash = "sha256-gWbhZc/BJwuiwyTMaR1ShEBI1lo0bGLCXsdj5n91baw=";
            subPackages = [ "cmd/golangci-lint" ];
            ldflags = [
              "-s"
              "-w"
              "-X main.version=2.15.0"
              "-X main.commit=25c5369a93aacb8cccea0d46e1569ca4a194564d"
              "-X main.date=2026-10-16T10:51:54Z"
            ];
          };
          modernize = buildGoModule {
            pname = "modernize";
            version = "0.52.0";
            src = pkgs.fetchFromGitHub {
              owner = "golang";
              repo = "tools";
              rev = "v0.52.0";
              hash = "sha256-invRApL81q9uIUCbBUhWGyS+14OKK6HZYdONzhHGKP4=";
            };
            vendorHash = "sha256-+1/eKOwN4yUvLCgGRJbW6MTleIEodfcIoEHCBRHd2MQ=";
            subPackages = [ "go/analysis/passes/modernize/cmd/modernize" ];
          };
          govulncheck = buildGoModule {
            pname = "govulncheck";
            version = "1.9.0";
            src = pkgs.fetchFromGitHub {
              owner = "golang";
              repo = "vuln";
              rev = "v1.9.0";
              hash = "sha256-XChu+f2RVp9flyk6R7n090Xbcgzi2ThQPuTqktkOC+U=";
            };
            vendorHash = "sha256-vPBwW2DtmJbX1UeJeC3J8wnRvq9+C/XDazbsXKkHkaQ=";
            subPackages = [ "cmd/govulncheck" ];
            # Command integration tests fetch fixture modules from the network.
            checkFlags = [ "-short" ];
          };
          gotestsum = buildGoModule {
            pname = "gotestsum";
            version = "1.13.0";
            src = pkgs.fetchFromGitHub {
              owner = "gotestyourself";
              repo = "gotestsum";
              rev = "v1.13.0";
              hash = "sha256-TTv1CUBG6rEgnb0NV3ntDAg2ofAL+1WysFMkH0cGrBI=";
            };
            vendorHash = "sha256-25AhWZiXhniZ6Gmw4J7psE/FfbS1j7Ncte0s43Xo98o=";
            subPackages = [ "." ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = [
              go
              pkgs.git
              pkgs.just
              pkgs.file
              pkgs.docker-client
              golangci-lint
              gotestsum
              govulncheck
              modernize
            ];

            GOTOOLCHAIN = "local";

            shellHook = ''
              export GOTOOLCHAIN=local
              echo "jetstream dev shell: Go ${goVersion} ($(go version))"
              echo "tools: just $(just --version | awk '{print $2}'), golangci-lint ${golangci-lint.version}, gotestsum ${gotestsum.version}, govulncheck ${govulncheck.version}, modernize ${modernize.version}"
            '';
          };
        });
    };
}
