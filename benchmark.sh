#!/bin/bash

# Benchmark script untuk test performa sebelum dan sesudah optimasi

echo "🚀 Flexurio API Performance Benchmark"
echo "======================================"
echo ""

# Cek apakah server sudah running
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "⚠️  Server tidak running. Silakan start server terlebih dahulu:"
    echo "   cargo run --release"
    exit 1
fi

# Cek apakah wrk terinstall
if ! command -v wrk &> /dev/null; then
    echo "⚠️  wrk belum terinstall. Install dengan:"
    echo "   brew install wrk"
    exit 1
fi

# URL endpoint untuk test (sesuaikan dengan endpoint Anda)
ENDPOINT="${1:-http://127.0.0.1:8080/banks?sort=id&ascending=false}"
THREADS="${2:-4}"
CONNECTIONS="${3:-1000}"
DURATION="${4:-60s}"

# Bearer token dari user
BEARER_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjEiLCJubSI6IkFkbWluIEZsZXh1cmlvIiwiZXhwIjoxNzU5ODkxNDE2LCJhdCI6MTc1OTgwNTAxNiwicmwiOiJiYW5rX3R5cGVzLzEyNyxiYW5rcy8xMjcsY3VzdG9tZXJzLzEyNyxmbHhfcm9sZXMvMTI3LGZseF91c2Vycy8xMjcscHJvZHVjdHMvMTI3LHNhbGVzLzEyNyxzYWxlc19pdGVtcy8xMjciLCJjcyI6IiJ9.j7_9HTZkcVlkoNXyrghKyt0A4IoEbADvVNeD_mzHtDI"

echo "📊 Test Configuration:"
echo "   Endpoint:    $ENDPOINT"
echo "   Threads:     $THREADS"
echo "   Connections: $CONNECTIONS"
echo "   Duration:    $DURATION"
echo ""

# Get memory before test
echo "💾 Memory Usage Before Test:"
ps aux | grep "flx-nocode" | grep -v grep | awk '{print "   RSS: " $6/1024 " MB, VSZ: " $5/1024 " MB"}'
echo ""

# Run benchmark
echo "⏱️  Running Benchmark..."
echo "======================================"
wrk -t$THREADS -c$CONNECTIONS -d$DURATION \
    -H "Authorization: Bearer $BEARER_TOKEN" \
    --latency \
    $ENDPOINT

echo ""
echo "💾 Memory Usage After Test:"
ps aux | grep "flx-nocode" | grep -v grep | awk '{print "   RSS: " $6/1024 " MB, VSZ: " $5/1024 " MB"}'
echo ""

echo "✅ Benchmark Complete!"
echo ""
echo "📈 Expected Results (After Optimization):"
echo "   - RPS: ~500+ (previously 60)"
echo "   - Memory: ~300MB (previously 9GB)"
echo "   - Latency: <100ms p99"
echo ""
echo "🔧 Manual Test Command:"
echo "   curl -X GET '$ENDPOINT' \\"
echo "     --header 'Authorization: Bearer $BEARER_TOKEN'"
