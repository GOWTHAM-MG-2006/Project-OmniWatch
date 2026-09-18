# OmniWatch — supply-chain targets (IND-8)
#
# Usage from repo root:
#   make build-reproducible   # two builds, same digest when SOURCE_DATE_EPOCH is fixed
#   make sbom                 # sbom.spdx.json (SPDX 2.3) via syft
#   make sign                 # cosign keyless (OIDC) sign of $(IMAGE)
#
# Reproducible epoch pattern:
#   SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) make build-reproducible
#
# From omniwatch-agent dir: plain `make <target>` works there too —
# see omniwatch-agent/Makefile (same targets, AGENT_DIR := .).

AGENT_DIR ?= omniwatch-agent
IMAGE ?= omniwatch-agent:latest
SBOM_FILE ?= sbom.spdx.json

# Default epoch = last commit time; override: SOURCE_DATE_EPOCH=123 make <t>
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || echo 0)

.PHONY: build-reproducible sbom sign verify verify-reproducible

build-reproducible:
	docker buildx build --progress=plain \
		--build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
		-t $(IMAGE) -f $(AGENT_DIR)/Dockerfile $(AGENT_DIR)

# Build twice with a fixed epoch and compare digests (reproducibility proof).
verify-reproducible:
	$(MAKE) build-reproducible IMAGE=$(IMAGE)-repro-a SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)
	$(MAKE) build-reproducible IMAGE=$(IMAGE)-repro-b SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)
	docker inspect --format='{{.Id}}' $(IMAGE)-repro-a > .repro-a.id
	docker inspect --format='{{.Id}}' $(IMAGE)-repro-b > .repro-b.id
	cmp .repro-a.id .repro-b.id && echo "REPRODUCIBLE: digests match" || (echo "NOT REPRODUCIBLE"; exit 1)
	rm -f .repro-a.id .repro-b.id

# SPDX 2.3 JSON SBOM. Installs syft on demand if missing (Linux/macOS/CI);
# on Windows without syft, install from https://github.com/anchore/syft/releases.
sbom: build-reproducible
	@command -v syft >/dev/null 2>&1 || { echo "syft not found — install it (see Makefile comment)"; exit 1; }
	syft $(IMAGE) -o spdx-json=$(SBOM_FILE)
	@python -c "import json;d=json.load(open('$(SBOM_FILE)'));assert d.get('spdxVersion','').startswith('SPDX-2.3'),d.get('spdxVersion');print('SBOM OK:',d.get('spdxVersion'),len(d.get('packages',[])),'packages')"

# Keyless (OIDC, no paid keys/HSM) sign. Requires cosign + OIDC env (CI provider).
sign:
	@command -v cosign >/dev/null 2>&1 || { echo "cosign not found — install from https://github.com/sigstore/cosign/releases"; exit 1; }
	cosign sign --yes $(IMAGE)

# Verify a keyless signature (also used as the E2E pipeline note step).
verify:
	@command -v cosign >/dev/null 2>&1 || { echo "cosign not found — install from https://github.com/sigstore/cosign/releases"; exit 1; }
	cosign verify $(IMAGE)
