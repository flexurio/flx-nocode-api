#!/bin/bash

# Mixed workload benchmark - Simultaneous GET and POST requests
# Simulates real-world production traffic

echo "🚀 Flexurio API Mixed Workload Benchmark"
echo "=========================================="
echo ""

# Cek apakah server sudah running
if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "⚠️  Server tidak running. Silakan start server terlebih dahulu:"
    echo "   cargo run --release"
    exit 1
fi

# Configuration
GET_ENDPOINT="http://127.0.0.1:8080/banks?sort=id&ascending=false"
POST_ENDPOINT="http://127.0.0.1:8080/banks"
BEARER_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjEiLCJubSI6IkFkbWluIEZsZXh1cmlvIiwiZXhwIjoxNzU5MjIzODUyLCJhdCI6MTc1OTEzNzQ1MiwicmwiOiJiYW5rX3R5cGVzLzEyNyxiYW5rcy8xMjcsY3VzdG9tZXJzLzEyNyxmbHhfcm9sZXMvMTI3LGZseF91c2Vycy8xMjcscHJvZHVjdHMvMTI3LHNhbGVzLzEyNyxzYWxlc19pdGVtcy8xMjciLCJjcyI6IiJ9.mVdIJVgLPwpAjoGi-t4uqRSSBg5wQyYmojdq5YHe0PU"

# Test parameters
DURATION="${1:-30}"           # Duration in seconds
GET_RATE="${2:-80}"           # Percentage of GET requests (default 80%)
GET_WORKERS="${3:-4}"         # Number of GET workers (concurrent threads)
POST_WORKERS="${4:-2}"        # Number of POST workers (concurrent threads)
CONCURRENT_USERS="${5:-100}"  # Number of concurrent users per worker
POST_RATE=$((100 - GET_RATE))
TOTAL_WORKERS=$((GET_WORKERS + POST_WORKERS))
TOTAL_CONNECTIONS=$((TOTAL_WORKERS * CONCURRENT_USERS))

echo "📊 Test Configuration:"
echo "   GET Endpoint:    $GET_ENDPOINT"
echo "   POST Endpoint:   $POST_ENDPOINT"
echo "   Duration:        ${DURATION}s"
echo "   GET Workers:     $GET_WORKERS (${GET_RATE}% traffic)"
echo "   POST Workers:    $POST_WORKERS (${POST_RATE}% traffic)"
echo "   Total Workers:   $TOTAL_WORKERS"
echo "   Concurrent Users: $CONCURRENT_USERS per worker"
echo "   Total Connections: $TOTAL_CONNECTIONS"
echo ""

# Function to generate random bank name
generate_random_name() {
    echo "BANK_AUTO_$(date +%s)_$RANDOM"
}

# Function to run GET requests with concurrent users
run_get_requests() {
    local duration=$1
    local concurrent=$2
    local count=0
    local success=0
    local failed=0
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    
    while [ $(date +%s) -lt $end_time ]; do
        # Launch concurrent requests
        for ((i=0; i<concurrent; i++)); do
            (
                response=$(curl -s -o /dev/null -w "%{http_code}" -X GET "$GET_ENDPOINT" \
                    --header "Authorization: Bearer $BEARER_TOKEN" 2>/dev/null)
                
                if [ "$response" = "200" ]; then
                    echo "SUCCESS" >> /tmp/benchmark_get_tmp_$$
                else
                    echo "FAILED" >> /tmp/benchmark_get_tmp_$$
                fi
            ) &
        done
        
        # Wait for batch to complete
        wait
        
        # Small delay between batches
        sleep 0.05
    done
    
    # Count results
    if [ -f /tmp/benchmark_get_tmp_$$ ]; then
        count=$(wc -l < /tmp/benchmark_get_tmp_$$)
        success=$(grep -c "SUCCESS" /tmp/benchmark_get_tmp_$$ 2>/dev/null || echo 0)
        failed=$(grep -c "FAILED" /tmp/benchmark_get_tmp_$$ 2>/dev/null || echo 0)
        rm -f /tmp/benchmark_get_tmp_$$
    fi
    
    echo "GET,$count,$success,$failed" > /tmp/benchmark_get_$$
}

# Function to run POST requests with concurrent users
run_post_requests() {
    local duration=$1
    local concurrent=$2
    local count=0
    local success=0
    local failed=0
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    
    while [ $(date +%s) -lt $end_time ]; do
        # Launch concurrent requests
        for ((i=0; i<concurrent; i++)); do
            (
                local random_name=$(generate_random_name)
                
                response=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POST_ENDPOINT" \
                    --header "Content-Type: multipart/form-data" \
                    --header "Authorization: Bearer $BEARER_TOKEN" \
                    --form "name=$random_name" \
                    --form "bank_type_id=BMG/2025/10/001" 2>/dev/null)
                
                if [ "$response" = "200" ] || [ "$response" = "201" ]; then
                    echo "SUCCESS" >> /tmp/benchmark_post_tmp_$$
                else
                    echo "FAILED" >> /tmp/benchmark_post_tmp_$$
                fi
            ) &
        done
        
        # Wait for batch to complete
        wait
        
        # Slightly longer delay for POST batches
        sleep 0.1
    done
    
    # Count results
    if [ -f /tmp/benchmark_post_tmp_$$ ]; then
        count=$(wc -l < /tmp/benchmark_post_tmp_$$)
        success=$(grep -c "SUCCESS" /tmp/benchmark_post_tmp_$$ 2>/dev/null || echo 0)
        failed=$(grep -c "FAILED" /tmp/benchmark_post_tmp_$$ 2>/dev/null || echo 0)
        rm -f /tmp/benchmark_post_tmp_$$
    fi
    
    echo "POST,$count,$success,$failed" > /tmp/benchmark_post_$$
}

# Get memory before test
echo "💾 Memory Usage Before Test:"
MEMORY_BEFORE=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
echo "   RSS: $(echo "scale=2; $MEMORY_BEFORE/1024" | bc) MB"
echo ""

# Run benchmark
echo "⏱️  Running Mixed Workload Benchmark..."
echo "=========================================="
echo ""

# Start both GET and POST workers in parallel
echo "🔄 Starting $GET_WORKERS GET workers with $CONCURRENT_USERS concurrent users each..."
for ((i=1; i<=GET_WORKERS; i++)); do
    run_get_requests $DURATION $CONCURRENT_USERS &
done

echo "📝 Starting $POST_WORKERS POST workers with $CONCURRENT_USERS concurrent users each..."
for ((i=1; i<=POST_WORKERS; i++)); do
    run_post_requests $DURATION $CONCURRENT_USERS &
done

echo ""
echo "⏳ Running for ${DURATION} seconds..."
echo "   Press Ctrl+C to stop early"
echo ""

# Show progress
for ((i=1; i<=$DURATION; i++)); do
    printf "\r   Progress: [%-50s] %d%%" $(printf '#%.0s' $(seq 1 $((i*50/DURATION)))) $((i*100/DURATION))
    sleep 1
done

echo ""
echo ""

# Wait for all workers to finish
wait

# Collect results
GET_TOTAL=0
GET_SUCCESS=0
GET_FAILED=0
POST_TOTAL=0
POST_SUCCESS=0
POST_FAILED=0

# Read GET results
if ls /tmp/benchmark_get_* 1> /dev/null 2>&1; then
    while IFS=, read -r type count success failed; do
        GET_TOTAL=$((GET_TOTAL + count))
        GET_SUCCESS=$((GET_SUCCESS + success))
        GET_FAILED=$((GET_FAILED + failed))
    done < <(cat /tmp/benchmark_get_*)
    rm -f /tmp/benchmark_get_*
fi

# Read POST results
if ls /tmp/benchmark_post_* 1> /dev/null 2>&1; then
    while IFS=, read -r type count success failed; do
        POST_TOTAL=$((POST_TOTAL + count))
        POST_SUCCESS=$((POST_SUCCESS + success))
        POST_FAILED=$((POST_FAILED + failed))
    done < <(cat /tmp/benchmark_post_*)
    rm -f /tmp/benchmark_post_*
fi

# Calculate metrics
TOTAL_REQUESTS=$((GET_TOTAL + POST_TOTAL))
TOTAL_SUCCESS=$((GET_SUCCESS + POST_SUCCESS))
TOTAL_FAILED=$((GET_FAILED + POST_FAILED))
GET_RPS=$(echo "scale=2; $GET_TOTAL/$DURATION" | bc)
POST_RPS=$(echo "scale=2; $POST_TOTAL/$DURATION" | bc)
TOTAL_RPS=$(echo "scale=2; $TOTAL_REQUESTS/$DURATION" | bc)
SUCCESS_RATE=$(echo "scale=2; $TOTAL_SUCCESS*100/$TOTAL_REQUESTS" | bc)

# Get memory after test
MEMORY_AFTER=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
MEMORY_GROWTH=$((MEMORY_AFTER - MEMORY_BEFORE))

# Display results
echo "✅ Benchmark Complete!"
echo ""
echo "📊 Results Summary"
echo "=========================================="
echo ""
echo "📈 Overall Metrics:"
echo "   Total Requests:    $TOTAL_REQUESTS"
echo "   Success:           $TOTAL_SUCCESS (${SUCCESS_RATE}%)"
echo "   Failed:            $TOTAL_FAILED"
echo "   Total RPS:         $TOTAL_RPS"
echo ""
echo "📥 GET Requests:"
echo "   Total:             $GET_TOTAL"
echo "   Success:           $GET_SUCCESS"
echo "   Failed:            $GET_FAILED"
echo "   RPS:               $GET_RPS"
echo ""
echo "📤 POST Requests:"
echo "   Total:             $POST_TOTAL"
echo "   Success:           $POST_SUCCESS"
echo "   Failed:            $POST_FAILED"
echo "   RPS:               $POST_RPS"
echo ""
echo "💾 Memory Usage:"
echo "   Before:            $(echo "scale=2; $MEMORY_BEFORE/1024" | bc) MB"
echo "   After:             $(echo "scale=2; $MEMORY_AFTER/1024" | bc) MB"
echo "   Growth:            $(echo "scale=2; $MEMORY_GROWTH/1024" | bc) MB"
echo ""

# Performance assessment
echo "🎯 Performance Assessment:"
if [ $(echo "$TOTAL_RPS > 400" | bc) -eq 1 ]; then
    echo "   ✅ RPS: EXCELLENT ($TOTAL_RPS > 400)"
elif [ $(echo "$TOTAL_RPS > 200" | bc) -eq 1 ]; then
    echo "   ⚠️  RPS: GOOD ($TOTAL_RPS)"
else
    echo "   ❌ RPS: NEEDS IMPROVEMENT ($TOTAL_RPS < 200)"
fi

if [ $(echo "$MEMORY_GROWTH < 100000" | bc) -eq 1 ]; then
    echo "   ✅ Memory: STABLE (growth < 100MB)"
elif [ $(echo "$MEMORY_GROWTH < 500000" | bc) -eq 1 ]; then
    echo "   ⚠️  Memory: MODERATE (growth < 500MB)"
else
    echo "   ❌ Memory: GROWING (growth > 500MB)"
fi

if [ $(echo "$SUCCESS_RATE > 99" | bc) -eq 1 ]; then
    echo "   ✅ Success Rate: EXCELLENT ($SUCCESS_RATE%)"
elif [ $(echo "$SUCCESS_RATE > 95" | bc) -eq 1 ]; then
    echo "   ⚠️  Success Rate: GOOD ($SUCCESS_RATE%)"
else
    echo "   ❌ Success Rate: POOR ($SUCCESS_RATE%)"
fi

echo ""
echo "📋 Expected Results (After Optimization):"
echo "   - Total RPS: ~500+"
echo "   - Memory Growth: <100MB"
echo "   - Success Rate: >99%"
echo ""

# Show sample commands
echo "🔧 Manual Test Commands:"
echo ""
echo "GET Request:"
echo "   curl -X GET '$GET_ENDPOINT' \\"
echo "     --header 'Authorization: Bearer $BEARER_TOKEN'"
echo ""
echo "POST Request:"
echo "   curl -X POST '$POST_ENDPOINT' \\"
echo "     --header 'Content-Type: multipart/form-data' \\"
echo "     --header 'Authorization: Bearer $BEARER_TOKEN' \\"
echo "     --form 'name=TEST_BANK_123' \\"
echo "     --form 'bank_type_id=BMG/2025/10/001'"
echo ""

# Usage help
echo "💡 Usage:"
echo "   ./benchmark-mixed.sh [duration] [get_rate] [get_workers] [post_workers] [concurrent_users]"
echo ""
echo "   Parameters:"
echo "   - duration:        Test duration in seconds (default: 30)"
echo "   - get_rate:        Percentage of GET traffic (default: 80)"
echo "   - get_workers:     Number of GET worker threads (default: 4)"
echo "   - post_workers:    Number of POST worker threads (default: 2)"
echo "   - concurrent_users: Concurrent requests per worker (default: 100)"
echo ""
echo "   Examples:"
echo "   ./benchmark-mixed.sh                        # Default: 30s, 80% GET, 4+2 workers, 100 concurrent"
echo "   ./benchmark-mixed.sh 60 80                  # 60s test, 80% GET/20% POST"
echo "   ./benchmark-mixed.sh 120 90 8 2             # 120s, 90% GET, 8 GET + 2 POST workers"
echo "   ./benchmark-mixed.sh 30 50 4 4              # 30s, 50/50 mix, equal workers"
echo "   ./benchmark-mixed.sh 60 70 10 5 50          # 60s, 70% GET, 15 workers, 50 concurrent each"
echo "   ./benchmark-mixed.sh 300 80 8 4 200         # 5min stress test, 1200 total connections"
