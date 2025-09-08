#!/bin/bash
"""
Test script for MapReduce programs
This script allows you to test the mapper and reducer locally before running on Hadoop
"""

# Make scripts executable
chmod +x src/mapper.py
chmod +x src/reducer.py

echo "Testing MapReduce programs locally..."
echo "======================================"

# Test with a small sample of data
echo "Running local test..."
head -100 data/e_commerce_transactions_dataset/ecommerce_transactions.csv | \
python3 src/mapper.py | \
sort | \
python3 src/reducer.py

echo ""
echo "Test completed. Check the logs above for mapper and reducer activity."
echo ""
echo "To run with Hadoop streaming, use:"
echo "hadoop jar \$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\"
echo "  -files src/mapper.py,src/reducer.py \\"
echo "  -mapper mapper.py \\"
echo "  -reducer reducer.py \\"
echo "  -input /path/to/input/data \\"
echo "  -output /path/to/output"
