# Compression codec benchmark (`bench-codec`)

slater compresses every `.blk` block with **zstd**, chosen once at build time and
decompressed many times on the read path. zstd's decode speed is ~independent of
the compression *level*, so a higher build level shrinks on-disk/on-wire bytes
(and thus read latency) for almost no extra decode cost — but only up to a knee,
past which the smaller GET no longer pays for the slower compress (build cost) or
the marginally slower decode. This benchmark finds that knee **per backend**, so
`slater-build --compression-profile` can be pinned to a measured level instead of
a guessed one.

There are two legs:

| Leg | What it measures | Where to run it |
|-----|------------------|-----------------|
| **CPU / ratio** | compression ratio, compress MB/s, decompress GB/s, zstd levels vs LZ4 | anywhere (laptop is fine) |
| **Backend I/O** | real GET latency at each level's byte size → total read time = GET + decompress → the knee | **on the backend's own network** — for S3 that means EC2 *in the bucket's region* |

> **Do not measure the S3 leg from a laptop, and do not measure it against MinIO.**
> A home-network / cross-region RTT (tens of ms) or a localhost MinIO (sub-ms, no
> real network) both push the knee to a wrong level. The S3 numbers are only valid
> from an instance sitting next to the bucket. That is the whole reason this ships
> as a container you run on EC2.

---

## 1. CPU / ratio leg — local, no S3 (the "is zstd still the right library?" answer)

Criterion micro-bench comparing zstd levels {1,3,9,19} against LZ4 on representative
block shapes (or real blocks if you point it at a generation):

```bash
# synthetic representative payloads
cargo bench -p graph-format --bench codec

# or against real decompressed blocks from a published local generation
SLATER_BENCH_GEN=/data/wikidata/<generation-uuid> cargo bench -p graph-format --bench codec
```

It prints a ratio table to stderr and Criterion throughput groups for
`decompress/<kind>` and `compress/<kind>`. Expect: zstd decode GB/s ~flat across
levels; LZ4 decode faster but at a much worse ratio (i.e. more bytes to read).

The `bench-codec` binary (below) also prints the CPU/ratio columns, so on a laptop
you can run it with `--no-io` to get ratio + decode speed without the misleading
local-disk GET numbers:

```bash
cargo run --release -p slater-build --bin bench-codec -- \
  --data-dir /data --graph wikidata --no-io --levels 1,3,6,9,12,15,19,22
```

---

## 2. Backend I/O leg — on EC2, against real S3

### 2a. Build and publish the image (from the laptop / CI)

The benchmark binary ships in the standard slater image (built with the `s3`
feature by default), alongside `slater` and `slater-build`:

```bash
# build (amd64 example; the release matrix also builds arm64)
docker build -t slater:bench .

# tag + push to a registry the EC2 instance can pull from.
# ECR is simplest because the instance role can authenticate without keys:
AWS_REGION=eu-west-2
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REPO=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/slater
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
docker tag slater:bench $REPO:bench
docker push $REPO:bench
```

(GHCR works too — `docker tag slater:bench ghcr.io/<org>/slater:bench && docker push …`.)

### 2b. Launch an EC2 instance in the bucket's region

- **Region:** the **same region as the S3 bucket** — this is the point.
- **Instance type:** a current-gen compute instance (e.g. `c7i.2xlarge` amd64 or
  `c7g.2xlarge` arm64); note it in the results so decode-CPU numbers are comparable.
  Match the arch to the image you pushed.
- **IAM:** attach an instance profile granting **read** on the bucket, e.g.
  `s3:GetObject` + `s3:ListBucket` on `arn:aws:s3:::<bucket>` / `…/<prefix>/*`.
  No static access keys — the AWS SDK default credential chain picks up the role.
- **Networking:** default VPC is fine; reads go to the S3 endpoint over the AWS
  backbone (optionally an S3 gateway VPC endpoint for the lowest, most stable RTT).

Install Docker and pull:

```bash
sudo dnf install -y docker && sudo systemctl start docker   # Amazon Linux 2023
aws ecr get-login-password --region $AWS_REGION \
  | sudo docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
sudo docker pull $REPO:bench
```

### 2c. Run the benchmark against the real bucket

```bash
sudo docker run --rm --entrypoint /app/bench-codec $REPO:bench \
  --graph wikidata \
  --s3-bucket my-slater-bucket \
  --s3-region $AWS_REGION \
  --s3-prefix prod \
  --levels 1,3,6,9,12,15,19,22 \
  --blocks-per-file 64 \
  --io-samples 64 \
  | tee codec-bench-$(hostname)-$(date +%F).txt
```

Notes:
- Omit `--generation` to resolve `<graph>/current`; pass `--generation <uuid>` to
  pin a specific image.
- Credentials come from the instance role automatically. If you must use static
  keys instead, pass them as the usual `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
  / `AWS_SESSION_TOKEN` env vars (`docker run -e …`). **Do not point `--s3-endpoint`
  at MinIO for a real measurement.**
- `--s3-path-style` only for S3-compatible servers that require it (not AWS S3).

Copy `codec-bench-*.txt` back off the instance (it is the result) and terminate
the instance.

### 2d. Reading the output

Per file-kind and in aggregate (raw-byte weighted), each level row shows ratio,
compressed size/block, compress MB/s, decompress GB/s, the measured GET ms for
that size, and **total ms/block = GET + decompress**. The lowest total is marked
`<- knee`; the aggregate prints `# recommended level for this backend: N`.

Feed that number back into the profile constants in
`crates/slater-build/src/main.rs` (`LOCAL_ZSTD_LEVEL` / `REMOTE_ZSTD_LEVEL` /
`MAX_ZSTD_LEVEL`). Run the local leg the same way (`--data-dir` instead of
`--s3-bucket`, on the NVMe target) to pin `LOCAL_ZSTD_LEVEL`.

---

## What the profile does with the result

`slater-build` records the chosen level and profile name in the generation
manifest (`zstd_level`, `compression_profile`). The reader needs nothing from
these — zstd streams are self-describing — so changing levels never requires a
format bump or a reader change. `--compression-profile auto` picks `remote` when a
`--publish-s3-bucket` target is set and `local` otherwise; `--zstd-level N`
overrides everything (recorded as `manual`).
