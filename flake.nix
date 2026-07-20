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
          goVersion = "1.26.5";
          go = assert pkgs.lib.assertMsg (goPkgs.go_1_26.version == goVersion)
            "nixpkgs-go must provide Go ${goVersion}, got ${goPkgs.go_1_26.version}";
            goPkgs.go_1_26;
          golangci-lint = pkgs.buildGoModule {
            pname = "golangci-lint";
            version = "2.10.1";
            src = pkgs.fetchFromGitHub {
              owner = "golangci";
              repo = "golangci-lint";
              rev = "v2.10.1";
              hash = "sha256-rHttQ+QJ9JrFvgfoX68Y0lD6BUv/aoOpRRFvZ1BIGIs=";
            };
            vendorHash = "sha256-yREpROQJ300+mii7R2oiyDjOGcYXBpv3o/park0TJYE=";
            subPackages = [ "cmd/golangci-lint" ];
            ldflags = [
              "-s"
              "-w"
              "-X main.version=2.10.1"
            ];
          };
          modernize = pkgs.buildGoModule {
            pname = "modernize";
            version = "0.20.0";
            src = pkgs.fetchFromGitHub {
              owner = "golang";
              repo = "tools";
              rev = "gopls/v0.20.0";
              hash = "sha256-DYYitsrdH4nujDFJgdkObEpgElhXI7Yk2IpA/EVVLVo=";
            };
            modRoot = "gopls";
            vendorHash = "sha256-J6QcefSs4XtnktlzG+/+aY6fqkHGd9MMZqi24jAwcd0=";
            subPackages = [ "internal/analysis/modernize/cmd/modernize" ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = [
              go
              pkgs.git
              pkgs.just
              pkgs.docker-client
              golangci-lint
              pkgs.gotestsum
              pkgs.govulncheck
              modernize
            ];

            GOTOOLCHAIN = "local";

            shellHook = ''
              export GOTOOLCHAIN=local
              echo "jetstream dev shell: Go ${goVersion} ($(go version))"
              echo "tools: just $(just --version | awk '{print $2}'), golangci-lint 2.10.1, gotestsum 1.13.0, govulncheck 1.5.0, modernize 0.20.0"
            '';
          };
        });
    };
}
