# omniwatch-agent — local wrapper so `make <target>` works from this dir too.
# Same targets as the repo-root Makefile (supply-chain: IND-8).
AGENT_DIR := .
IMAGE ?= omniwatch-agent:latest
SBOM_FILE ?= sbom.spdx.json
SOURCE_DATE_EPOCH ?= $(shell git log -1 --format=%ct 2>/dev/null || echo 0)

.PHONY: build-reproducible sbom sign verify verify-reproducible

build-reproducible:
	docker buildx build --load --progress=plain \
		--build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH) \
		-t $(IMAGE) -f Dockerfile .

verify-reproducible:
	$(MAKE) build-reproducible IMAGE=$(IMAGE)-repro-a SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)
	$(MAKE) build-reproducible IMAGE=$(IMAGE)-repro-b SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)
	docker inspect --format='{{.Id}}' $(IMAGE)-repro-a > .repro-a.id
	docker inspect --format='{{.Id}}' $(IMAGE)-repro-b > .repro-b.id
	cmp .repro-a.id .repro-b.id && echo "REPRODUCIBLE: digests match" || (echo "NOT REPRODUCIBLE"; exit 1)
	rm -f .repro-a.id .repro-b.id

sbom: build-reproducible
	@command -v syft >/dev/null 2>&1 || { echo "syft not found — install it (see root Makefile comment)"; exit 1; }
	syft $(IMAGE) -o spdx-json=$(SBOM_FILE)
	@python -c "import json;d=json.load(open('$(SBOM_FILE)'));assert d.get('spdxVersion','').startswith('SPDX-2.3'),d.get('spdxVersion');print('SBOM OK:',d.get('spdxVersion'),len(d.get('packages',[])),'packages')"

sign:
	@command -v cosign >/dev/null 2>&1 || { echo "cosign not found — install from https://github.com/sigstore/cosign/releases"; exit 1; }
	cosign sign --yes $(IMAGE)

verify:
	@command -v cosign >/dev/null 2>&1 || { echo "cosign not found — install from https://github.com/sigstore/cosign/releases"; exit 1; }
	cosign verify $(IMAGE)
