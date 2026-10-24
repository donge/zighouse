#!/bin/bash
# Launch the standard ClickBench c6a.4xlarge host, run ClickBench, collect results.
# Required: AWS_AMI_ID (Ubuntu 24.04 x86_64), AWS_KEY_NAME,
#           AWS_SECURITY_GROUP_ID, AWS_SUBNET_ID.
# Usage: AWS_AMI_ID=... AWS_KEY_NAME=... AWS_SECURITY_GROUP_ID=... \
#        AWS_SUBNET_ID=... bash scripts/run-clickbench-aws.sh
set -euo pipefail

REGION=${AWS_REGION:-us-east-1}
AMI=${AWS_AMI_ID:?Set AWS_AMI_ID to an Ubuntu 24.04 x86_64 AMI}
INSTANCE_TYPE=${AWS_INSTANCE_TYPE:-c6a.4xlarge}
KEY_NAME=${AWS_KEY_NAME:?Set AWS_KEY_NAME}
SECURITY_GROUP=${AWS_SECURITY_GROUP_ID:?Set AWS_SECURITY_GROUP_ID}
SUBNET=${AWS_SUBNET_ID:?Set AWS_SUBNET_ID}
VOLUME_SIZE=${AWS_VOLUME_SIZE_GIB:-500}
VOLUME_TYPE=${AWS_VOLUME_TYPE:-gp2}
TODAY=$(date +%Y%m%d)

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ZIGHOUSE_DIR="${SCRIPT_DIR}/../clickbench-submit/zighouse"

# ---------- User-data: install deps, download hits.parquet, clone ClickBench ----------
cat > /tmp/zighouse-bench-userdata.sh << 'USERDATA_SCRIPT'
#!/bin/bash
set -ex
exec > /var/log/zighouse-bench-setup.log 2>&1
echo "=== Benchmark setup started $(date) ==="

apt-get update -qq
apt-get install -y -qq curl git gzip wget

mkdir -p /opt/clickbench
cd /opt/clickbench

echo "=== Downloading hits.parquet ==="
wget --progress=dot:giga -O hits.parquet \
  'https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet'

echo "=== Cloning ClickBench ==="
git clone --depth 1 https://github.com/ClickHouse/ClickBench.git clickbench-repo

echo "=== Setup done, awaiting zighouse files $(date) ==="
echo "READY" > /opt/clickbench/ready.txt
USERDATA_SCRIPT

# ---------- Launch instance ----------
echo "=== Launching $INSTANCE_TYPE in $REGION ==="

INSTANCE_ID=$(aws ec2 run-instances \
  --region "$REGION" \
  --image-id "$AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SECURITY_GROUP" \
  --subnet-id "$SUBNET" \
  --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$VOLUME_SIZE,\"VolumeType\":\"$VOLUME_TYPE\",\"DeleteOnTermination\":true}}]" \
  --user-data "file:///tmp/zighouse-bench-userdata.sh" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=zighouse-clickbench-${TODAY}}]" \
  --instance-initiated-shutdown-behavior terminate \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instance ID: $INSTANCE_ID"

# ---------- Wait for running ----------
echo "Waiting for instance to be running..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
sleep 5

PUBLIC_IP=$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text)
echo "Public IP: $PUBLIC_IP"

# ---------- Wait for SSH ----------
echo "Waiting for SSH..."
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"
for i in $(seq 1 30); do
  if ssh $SSH_OPTS ubuntu@"$PUBLIC_IP" "echo ok" 2>/dev/null; then
    echo "SSH ready"
    break
  fi
  echo "  attempt $i/30..."
  sleep 10
done

# ---------- Wait for user-data setup ----------
echo "Waiting for setup (hits.parquet download + ClickBench clone)..."
for i in $(seq 1 60); do
  if ssh $SSH_OPTS ubuntu@"$PUBLIC_IP" "test -f /opt/clickbench/ready.txt" 2>/dev/null; then
    echo "Setup complete"
    break
  fi
  [ $((i % 6)) -eq 0 ] && echo "  still waiting... ($i)"
  sleep 10
done

# ---------- Copy zighouse files ----------
echo "=== Copying zighouse entry files ==="
ssh $SSH_OPTS ubuntu@"$PUBLIC_IP" "rm -rf /opt/clickbench/clickbench-repo/zighouse"
scp $SSH_OPTS -r "${ZIGHOUSE_DIR}" "ubuntu@${PUBLIC_IP}:/opt/clickbench/clickbench-repo/zighouse"

# ---------- Setup and run benchmark ----------
echo "=== Running benchmark ==="
ssh $SSH_OPTS ubuntu@"$PUBLIC_IP" "bash -s" << 'REMOTE_BENCH'
set -ex

cd /opt/clickbench/clickbench-repo

# Create local download script that copies our pre-downloaded parquet
cat > lib/download-hits-parquet-single-local << 'DL'
#!/bin/bash
set -e
cp /opt/clickbench/hits.parquet ./hits.parquet
chmod 644 hits.parquet
echo "Using cached hits.parquet"
DL
chmod +x lib/download-hits-parquet-single-local
chmod +x zighouse/install zighouse/load zighouse/query zighouse/start zighouse/stop zighouse/check zighouse/data-size zighouse/benchmark.sh zighouse/generic-smoke.sh

cd zighouse

# Export the download script name (benchmark-common.sh looks in lib/)
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single-local"

# Run the full benchmark
bash benchmark.sh 2>&1 | tee /opt/clickbench/benchmark-output.txt
echo "=== DONE ==="

# Copy result files
rm -rf /opt/clickbench/results
cp -r results /opt/clickbench/results/
echo "BENCHMARK_COMPLETE" > /opt/clickbench/complete.txt
REMOTE_BENCH

echo ""
echo "=== Collecting results ==="

RESULTS_DIR="${ZIGHOUSE_DIR}/results/${TODAY}"
mkdir -p "$RESULTS_DIR"

# Copy results
scp $SSH_OPTS -r "ubuntu@${PUBLIC_IP}:/opt/clickbench/results/"* "$RESULTS_DIR/" 2>/dev/null || true
scp $SSH_OPTS "ubuntu@${PUBLIC_IP}:/opt/clickbench/benchmark-output.txt" "$RESULTS_DIR/benchmark-output.txt" 2>/dev/null || true

echo "Results saved to: $RESULTS_DIR"
echo ""

# Show summary
echo "=== Results Summary ==="
ls -la "$RESULTS_DIR/"
echo ""

if [ -f "$RESULTS_DIR/benchmark-output.txt" ]; then
  echo "--- Benchmark output (first 50 lines) ---"
  head -50 "$RESULTS_DIR/benchmark-output.txt"
fi

echo ""
echo "=== Instance info ==="
echo "  Instance: $INSTANCE_ID"
echo "  IP: $PUBLIC_IP"
echo "  Region: $REGION"
echo ""
echo "  To SSH:   ssh $SSH_OPTS ubuntu@$PUBLIC_IP"
echo "  Terminate: aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo ""
