#!/usr/bin/env python3
"""
Test script for Vortex memory optimization integration
"""

import logging
import os
import platform
import sys
from typing import Any, Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Simulate VORTEX_AVAILABLE
VORTEX_AVAILABLE = True  # Set to True to test with optimizations enabled

def _get_memory_allocator_info() -> Dict[str, Any]:
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


def _optimize_memory_allocator() -> None:
    """Apply memory allocator optimizations for Vortex performance."""
    system = platform.system()

    logger.info("🔧 Optimizing Memory Allocator for Vortex Performance")

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

    # Log applied optimizations
    optimizations = []
    if os.environ.get("MALLOC_ARENA_MAX"):
        optimizations.append(f"MALLOC_ARENA_MAX={os.environ['MALLOC_ARENA_MAX']}")
    if os.environ.get("MALLOC_MMAP_THRESHOLD"):
        threshold_kb = int(os.environ["MALLOC_MMAP_THRESHOLD"]) // 1024
        optimizations.append(f"MALLOC_MMAP_THRESHOLD={threshold_kb}KB")
    if os.environ.get("PYTHONMALLOC"):
        optimizations.append(f"PYTHONMALLOC={os.environ['PYTHONMALLOC']}")

    if optimizations:
        logger.info(f"✅ Applied memory optimizations: {', '.join(optimizations)}")
    else:
        logger.info("ℹ️  No additional memory optimizations needed")


def main():
    """Test the memory optimization integration."""
    print("🧪 Testing Vortex Memory Optimization Integration")
    print("=" * 50)

    print(f"System: {platform.system()}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Vortex Available: {VORTEX_AVAILABLE}")
    print()

    # Show current settings
    info = _get_memory_allocator_info()
    print("Current Memory Settings:")
    for var, value in info["current_settings"].items():
        print(f"  {var}: {value}")
    print()

    # Apply optimizations (simulate module loading)
    if VORTEX_AVAILABLE:
        try:
            _optimize_memory_allocator()
            print("✅ Memory optimizations applied successfully")
        except Exception as e:
            print(f"⚠️  Failed to apply optimizations: {e}")
    else:
        print("ℹ️  Vortex not available, optimizations would be skipped")
        # But let's still show what would happen
        print("   (Simulating optimization application...)")
        _optimize_memory_allocator()

    print()
    print("🎉 Memory optimization integration test completed!")


if __name__ == "__main__":
    main()
