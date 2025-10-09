#!/bin/bash

# Script untuk test performa dengan berbagai skenario

BEARER_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjEiLCJubSI6IkFkbWluIEZsZXh1cmlvIiwiZXhwIjoxNzU5ODkxNDE2LCJhdCI6MTc1OTgwNTAxNiwicmwiOiJiYW5rX3R5cGVzLzEyNyxiYW5rcy8xMjcsY3VzdG9tZXJzLzEyNyxmbHhfcm9sZXMvMTI3LGZseF91c2Vycy8xMjcscHJvZHVjdHMvMTI3LHNhbGVzLzEyNyxzYWxlc19pdGVtcy8xMjciLCJjcyI6IiJ9.j7_9HTZkcVlkoNXyrghKyt0A4IoEbADvVNeD_mzHtDI"

echo "🧪 Flexurio API Performance Test Suite"
echo "======================================="
echo ""

# Test 1: Single request untuk verify endpoint works
echo "📝 Test 1: Single Request Test"
echo "------------------------------"
RESPONSE=$(curl -s -w "\n%{http_code}\n%{time_total}" -X GET \
  'http://127.0.0.1:8080/banks?sort=id&ascending=false' \
  --header "Authorization: Bearer $BEARER_TOKEN")

HTTP_CODE=$(echo "$RESPONSE" | tail -n 2 | head -n 1)
TIME_TOTAL=$(echo "$RESPONSE" | tail -n 1)
BODY=$(echo "$RESPONSE" | head -n -2)

echo "   Status Code: $HTTP_CODE"
echo "   Response Time: ${TIME_TOTAL}s"
echo "   Response Body (first 200 chars):"
echo "$BODY" | head -c 200
echo ""
echo ""

if [ "$HTTP_CODE" != "200" ]; then
    echo "❌ Error: Expected 200, got $HTTP_CODE"
    echo "Response:"
    echo "$BODY"
    exit 1
fi

echo "✅ Endpoint is working correctly"
echo ""

# Test 2: Concurrent requests test
echo "📝 Test 2: Concurrent Requests (100 requests)"
echo "---------------------------------------------"
START_MEM=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
echo "   Memory before: $(echo "scale=2; $START_MEM/1024" | bc) MB"

START_TIME=$(date +%s)
for i in {1..100}; do
    curl -s -X GET 'http://127.0.0.1:8080/banks?sort=id&ascending=false' \
      --header "Authorization: Bearer $BEARER_TOKEN" > /dev/null &
done
wait
END_TIME=$(date +%s)

END_MEM=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
DURATION=$((END_TIME - START_TIME))
RPS=$(echo "scale=2; 100/$DURATION" | bc)

echo "   Duration: ${DURATION}s"
echo "   RPS: $RPS"
echo "   Memory after: $(echo "scale=2; $END_MEM/1024" | bc) MB"
echo "   Memory growth: $(echo "scale=2; ($END_MEM-$START_MEM)/1024" | bc) MB"
echo ""

# Test 3: Sustained load test
echo "📝 Test 3: Sustained Load (10 seconds)"
echo "--------------------------------------"
START_MEM=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
echo "   Memory before: $(echo "scale=2; $START_MEM/1024" | bc) MB"

COUNT=0
START_TIME=$(date +%s)
END_TARGET=$((START_TIME + 10))

while [ $(date +%s) -lt $END_TARGET ]; do
    curl -s -X GET 'http://127.0.0.1:8080/banks?sort=id&ascending=false' \
      --header "Authorization: Bearer $BEARER_TOKEN" > /dev/null &
    COUNT=$((COUNT + 1))
    
    # Limit concurrent requests
    if [ $((COUNT % 20)) -eq 0 ]; then
        wait
    fi
done
wait

END_TIME=$(date +%s)
END_MEM=$(ps aux | grep "flx-nocode" | grep -v grep | awk '{print $6}')
DURATION=$((END_TIME - START_TIME))
RPS=$(echo "scale=2; $COUNT/$DURATION" | bc)

echo "   Total requests: $COUNT"
echo "   Duration: ${DURATION}s"
echo "   Average RPS: $RPS"
echo "   Memory after: $(echo "scale=2; $END_MEM/1024" | bc) MB"
echo "   Memory growth: $(echo "scale=2; ($END_MEM-$START_MEM)/1024" | bc) MB"
echo ""

# Summary
echo "📊 Summary"
echo "=========="
echo "   Single request latency: ${TIME_TOTAL}s"
echo "   Concurrent 100 req RPS: $RPS"
echo "   Memory seems: $(if [ $(echo "$END_MEM - $START_MEM < 100000" | bc) -eq 1 ]; then echo "✅ STABLE"; else echo "⚠️  GROWING"; fi)"
echo ""

# Recommendations
if [ $(echo "$RPS < 100" | bc) -eq 1 ]; then
    echo "⚠️  Warning: RPS is below 100. Expected ~500+"
    echo ""
    echo "Possible issues:"
    echo "   1. Check if optimizations are applied correctly"
    echo "   2. Run flamegraph to identify bottlenecks"
    echo "   3. Check database connection pool settings"
elif [ $(echo "$RPS < 300" | bc) -eq 1 ]; then
    echo "⚠️  RPS is moderate (~$RPS). Expected ~500+"
    echo "   Consider checking database query optimization"
else
    echo "✅ Performance looks good! RPS: $RPS"
fi

echo ""
echo "💡 For detailed load testing, run:"
echo "   ./benchmark.sh"
