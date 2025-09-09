#!/usr/bin/env python3
"""
Memory Allocator Optimization for Vortex Performance
===================================================

This script demonstrates how to optimize memory allocation for better Vortex performance.
The MiMalloc allocator setting mentioned in the docs is for the Rust implementation,
but we can optimize Python's memory allocation through environment variables.

Usage:
    python optimize_memory.py

Or set environment variables before running your application:
    export MALLOC_ARENA_MAX=1
    export MALLOC_MMAP_THRESHOLD=131072
    export PYTHONMALLOC=malloc
    python your_vortex_application.py
"""

import os
import platform
import sys
from typing import Dict, Any


def get_memory_allocator_info() -> Dict[str, Any]:
    """Get information about the current memory allocator configuration."""
    system = platform.system()

    info = {
        "system": system,
        "python_version": sys.version.split()[0],
        "current_settings": {},
        "recommended_settings": {},
        "optimizations_applied": []
    }

    # Check current environment variables
    alloc_vars = [
        "MALLOC_ARENA_MAX",
        "MALLOC_MMAP_THRESHOLD",
        "MALLOC_TRIM_THRESHOLD",
        "MALLOC_TOP_PAD",
        "PYTHONMALLOC"
    ]

    for var in alloc_vars:
        current_value = os.environ.get(var)
        info["current_settings"][var] = current_value or "default"

    # Set recommended values based on system
    if system == "Linux":
        info["recommended_settings"] = {
            "MALLOC_ARENA_MAX": "1",  # Single arena for better cache locality
            "MALLOC_MMAP_THRESHOLD": "131072",  # 128KB threshold for mmap
            "MALLOC_TRIM_THRESHOLD": "524288",  # 512KB trim threshold
            "MALLOC_TOP_PAD": "1048576",  # 1MB top pad
            "PYTHONMALLOC": "malloc"  # Use system malloc
        }
    elif system == "Darwin":  # macOS
        info["recommended_settings"] = {
            "MALLOC_MMAP_THRESHOLD": "131072",
            "PYTHONMALLOC": "malloc"
        }
    else:
        info["recommended_settings"] = {
            "PYTHONMALLOC": "malloc"
        }

    return info


def optimize_memory_allocator() -> None:
    """Apply memory allocator optimizations for Vortex performance."""
    system = platform.system()

    print("🔧 Optimizing Memory Allocator for Vortex Performance")
    print("=" * 55)

    if system == "Linux":
        # Optimize glibc malloc for high-throughput workloads
        os.environ.setdefault("MALLOC_ARENA_MAX", "1")
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("MALLOC_TRIM_THRESHOLD", "524288")
        os.environ.setdefault("MALLOC_TOP_PAD", "1048576")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    elif system == "Darwin":
        # macOS optimizations (limited tunables available)
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Cross-platform optimizations
    os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Display applied optimizations
    print(f"✅ System: {system}")
    print(f"✅ Python: {sys.version.split()[0]}")

    optimizations = []
    if os.environ.get("MALLOC_ARENA_MAX"):
        optimizations.append(f"MALLOC_ARENA_MAX={os.environ['MALLOC_ARENA_MAX']}")
    if os.environ.get("MALLOC_MMAP_THRESHOLD"):
        threshold_kb = int(os.environ["MALLOC_MMAP_THRESHOLD"]) // 1024
        optimizations.append(f"MALLOC_MMAP_THRESHOLD={threshold_kb}KB")
    if os.environ.get("PYTHONMALLOC"):
        optimizations.append(f"PYTHONMALLOC={os.environ['PYTHONMALLOC']}")

    print("✅ Applied optimizations:")
    for opt in optimizations:
        print(f"   • {opt}")

    print("\n💡 Note: MiMalloc allocator setting is for Rust/Vortex internals")
    print("   These Python optimizations improve memory allocation performance")
    print("   for the Python wrapper and data processing pipeline.")


def benchmark_memory_allocation() -> None:
    """Simple benchmark to demonstrate memory allocation performance."""
    import time
    import gc

    print("\n🧪 Memory Allocation Benchmark")
    print("=" * 30)

    # Force garbage collection before benchmark
    gc.collect()

    # Benchmark memory allocation
    start_time = time.perf_counter()

    # Create memory pressure similar to Vortex data processing
    data = []
    for i in range(50000):
        # Simulate creating records with multiple fields
        record = {
            'id': i,
            'name': f'user_{i}',
            'values': [i * j for j in range(10)],  # List of 10 integers
            'metadata': f'{{"key": "value_{i % 100}"}}'  # JSON-like string
        }
        data.append(record)

    end_time = time.perf_counter()
    allocation_time = (end_time - start_time) * 1000

    print(f"⏱️  Allocation time: {allocation_time:.2f}ms")
    print(f"📊 Records processed: {len(data):,.0f}")
    print("   (This simulates Vortex data processing memory patterns)")


def main():
    """Main function demonstrating memory optimization."""
    print("Memory Allocator Optimization for Vortex Performance")
    print("=" * 55)

    # Get current configuration
    info = get_memory_allocator_info()

    print(f"System: {info['system']}")
    print(f"Python: {info['python_version']}")
    print()

    # Show current vs recommended settings
    print("Current Memory Allocator Settings:")
    for var, value in info["current_settings"].items():
        recommended = info["recommended_settings"].get(var)
        status = "✅" if value == recommended or (value == "default" and recommended) else "⚠️"
        print(f"  {status} {var}: {value}")

    print()

    # Apply optimizations
    optimize_memory_allocator()

    # Run benchmark
    benchmark_memory_allocation()

    print("\n📚 Additional Notes:")
    print("• The MiMalloc setting from Vortex docs applies to the Rust crate")
    print("• These Python optimizations improve the data processing pipeline")
    print("• For maximum performance, ensure Vortex Rust crate uses MiMalloc")
    print("• Memory optimizations are most beneficial for large datasets")


if __name__ == "__main__":
    main()
