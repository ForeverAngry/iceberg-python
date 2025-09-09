#!/usr/bin/env python3
"""
Test script for Scalene profiling setup verification
===================================================

This script creates a simple workload that can be profiled with Scalene
to verify that the profiling configuration is working correctly.
"""

import random
import time
from typing import Dict, List


def create_test_data(num_records: int = 10000) -> List[Dict]:
    """Create test data similar to what Vortex might process."""
    data = []
    for i in range(num_records):
        record = {
            'id': i,
            'name': f'user_{i}',
            'email': f'user_{i}@example.com',
            'age': random.randint(18, 80),
            'score': random.random() * 100,
            'tags': [f'tag_{j}' for j in range(random.randint(1, 5))],
            'metadata': {
                'created_at': time.time(),
                'category': random.choice(['A', 'B', 'C', 'D']),
                'active': random.choice([True, False])
            }
        }
        data.append(record)
    return data


def process_data_cpu_intensive(data: List[Dict]) -> Dict:
    """CPU-intensive data processing function."""
    results = {}

    # Simulate complex data processing
    for record in data:
        # CPU-intensive operations
        score = record['score']
        processed_score = 0

        # Simulate complex calculations
        for _ in range(100):
            processed_score += score * random.random()
            processed_score = processed_score ** 0.5  # Square root
            processed_score = processed_score * 2 + 1

        # String processing
        name_hash = hash(record['name']) % 1000
        email_parts = record['email'].split('@')
        domain_hash = hash(email_parts[1]) % 100

        # Tag processing
        tag_scores = [hash(tag) % 100 for tag in record['tags']]
        avg_tag_score = sum(tag_scores) / len(tag_scores) if tag_scores else 0

        results[record['id']] = {
            'processed_score': processed_score,
            'name_hash': name_hash,
            'domain_hash': domain_hash,
            'avg_tag_score': avg_tag_score,
            'category': record['metadata']['category']
        }

    return results


def process_data_memory_intensive(data: List[Dict]) -> List[Dict]:
    """Memory-intensive data processing function."""
    processed_data = []

    # Create multiple copies and transformations
    for record in data:
        # Create multiple variations of the record
        variations = []
        for i in range(10):
            variation = record.copy()
            variation['variation_id'] = i
            variation['transformed_score'] = record['score'] * (i + 1)
            variation['duplicated_tags'] = record['tags'] * 2
            variation['large_string'] = 'x' * 1000  # 1KB string
            variations.append(variation)

        # Combine all variations
        combined = {
            'original_id': record['id'],
            'variations': variations,
            'summary': {
                'total_variations': len(variations),
                'avg_score': sum(v['transformed_score'] for v in variations) / len(variations),
                'all_tags': [tag for v in variations for tag in v['duplicated_tags']]
            }
        }
        processed_data.append(combined)

    return processed_data


def simulate_vortex_operations():
    """Simulate typical Vortex file format operations."""
    print("🔄 Simulating Vortex operations...")

    # Create test data
    print("📊 Creating test data...")
    data = create_test_data(5000)

    # CPU-intensive processing
    print("⚡ Running CPU-intensive processing...")
    cpu_results = process_data_cpu_intensive(data)

    # Memory-intensive processing
    print("🧠 Running memory-intensive processing...")
    memory_results = process_data_memory_intensive(data[:1000])  # Smaller subset for memory ops

    # Simulate file I/O operations
    print("💾 Simulating file I/O operations...")
    for i in range(100):
        # Simulate writing/reading operations
        temp_data = data[i:i+10]  # Small batch
        # Simulate serialization/deserialization
        serialized = str(temp_data)
        _ = eval(serialized)  # Note: eval is for demo only, result not used

    print("✅ Vortex simulation completed")
    return {
        'cpu_results_count': len(cpu_results),
        'memory_results_count': len(memory_results),
        'total_records_processed': len(data)
    }


def main():
    """Main function for the profiling test."""
    print("🚀 Scalene Profiling Test Script")
    print("=" * 40)
    print("This script creates workloads suitable for Scalene profiling.")
    print("Use with: python profile_scalene.py python scalene_test.py")
    print()

    start_time = time.time()

    # Run the simulation
    results = simulate_vortex_operations()

    end_time = time.time()
    duration = end_time - start_time

    print("\n📊 Test Results:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   CPU results: {results['cpu_results_count']}")
    print(f"   Memory results: {results['memory_results_count']}")
    print(f"   Total records: {results['total_records_processed']}")
    print(".2f")
    print("\n🎯 This workload is designed to:")
    print("   • Exercise CPU-intensive operations")
    print("   • Create memory allocation patterns")
    print("   • Simulate I/O operations")
    print("   • Test profiling robustness")


if __name__ == "__main__":
    main()
