#!/usr/bin/env python3
"""
Scalene Profiling Script for PyIceberg
======================================

This script provides robust profiling capabilities for PyIceberg using Scalene.
It can identify and profile specific processes, handle complex applications,
and provide detailed performance analysis.

Usage:
    python profile_scalene.py [options] <command>

Examples:
    # Profile a specific script
    python profile_scalene.py python my_script.py

    # Profile with custom output directory
    python profile_scalene.py --output-dir ./profiles python benchmark.py

    # Profile only CPU usage
    python profile_scalene.py --cpu-only python data_processing.py

    # Profile with memory leak detection
    python profile_scalene.py --memory-leak python long_running_app.py

    # Profile specific modules
    python profile_scalene.py --modules pyiceberg.io python vortex_test.py
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional


class ScaleneProfiler:
    """Robust Scalene profiler for PyIceberg applications."""

    def __init__(self, output_dir: str = ".bench_out/scalene"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = time.strftime("%Y%m%d-%H%M%S")

    def _get_scalene_command(self, args: argparse.Namespace) -> List[str]:
        """Build the Scalene command with appropriate options."""
        cmd = ["poetry", "run", "scalene"]

        # Basic profiling options - use options that exist in this version
        cmd.extend(["--html", "--json", "--cli"])

        # Sampling rates
        if args.cpu_sampling_rate:
            cmd.extend(["--cpu-sampling-rate", str(args.cpu_sampling_rate)])

        if args.memory_sampling_rate:
            cmd.extend(["--memory-sampling-rate", str(args.memory_sampling_rate)])

        # Output options
        cmd.extend(["--html", "--json", "--reduced-profile"])

        # Process identification and focus
        if args.pid:
            cmd.extend(["--pid", str(args.pid)])

        if args.modules:
            # Focus profiling on specific modules
            for module in args.modules:
                cmd.extend(["--profile-only", module])

        # Memory leak detection
        if args.memory_leak:
            cmd.extend(["--memory-leak-detector"])

        # Output file
        output_file = self.output_dir / f"scalene_profile_{self.timestamp}"
        cmd.extend(["--output-file", str(output_file)])

        # Web UI (disabled for headless operation)
        cmd.extend(["--no-web"])

        # Advanced options for robustness
        cmd.extend([
            "--suppress-profile-errors",
            "--profile-threads",
            "--profile-copy"
        ])

        return cmd

    def _find_process_by_name(self, name: str) -> Optional[int]:
        """Find a process by name for targeted profiling."""
        try:
            import psutil

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if name.lower() in proc.info['name'].lower():
                        return proc.info['pid']
                    # Check command line as well
                    if proc.info['cmdline']:
                        cmdline = ' '.join(proc.info['cmdline'])
                        if name.lower() in cmdline.lower():
                            return proc.info['pid']
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except ImportError:
            print("⚠️  psutil not available. Install with: pip install psutil")
        except Exception as e:
            print(f"⚠️  Error finding process: {e}")

        return None

    def _setup_environment(self):
        """Set up environment variables for optimal profiling."""
        # Memory allocator optimizations (already handled in vortex.py)
        # But we can add additional profiling-specific settings

        # Ensure Python optimizations are enabled
        os.environ.setdefault("PYTHONOPTIMIZE", "1")

        # Disable Python's garbage collector during profiling for cleaner results
        if os.environ.get("SCALENE_DISABLE_GC"):
            os.environ.setdefault("PYTHONMALLOC", "malloc")

        # Set profiling-specific environment variables
        os.environ.setdefault("SCALENE_PROFILE_ALL", "false")
        os.environ.setdefault("SCALENE_USE_VIRTUAL_TIME", "false")

    def profile_command(self, command: List[str], args: argparse.Namespace) -> int:
        """Profile a command using Scalene."""
        print("🔬 Scalene Profiling Setup")
        print("=" * 40)

        # Setup environment
        self._setup_environment()

        # Build Scalene command
        scalene_cmd = self._get_scalene_command(args)
        full_cmd = scalene_cmd + ["--"] + command

        print(f"📊 Profiling command: {' '.join(command)}")
        print(f"🎯 Output directory: {self.output_dir}")
        print(f"📈 Profile timestamp: {self.timestamp}")
        print()

        # Execute profiling
        try:
            print("🚀 Starting Scalene profiling...")
            result = subprocess.run(full_cmd, cwd=os.getcwd())

            # Check for output files
            self._check_output_files()

            return result.returncode

        except KeyboardInterrupt:
            print("\n⏹️  Profiling interrupted by user")
            return 130
        except Exception as e:
            print(f"❌ Profiling failed: {e}")
            return 1

    def profile_process(self, pid: int, duration: int = 60) -> int:
        """Profile a running process by PID."""
        print(f"🔬 Profiling process PID: {pid}")
        print(f"⏱️  Duration: {duration} seconds")
        print()

        try:
            # Use scalene to attach to running process
            cmd = [
                "poetry", "run", "scalene", "--pid", str(pid),
                "--html", "--json", "--cli",
                "--outfile", str(self.output_dir / f"scalene_pid_{pid}_{self.timestamp}.txt")
            ]

            print("🚀 Attaching to process...")
            result = subprocess.run(cmd, timeout=duration)

            self._check_output_files()
            return result.returncode

        except subprocess.TimeoutExpired:
            print(f"✅ Profiling completed after {duration} seconds")
            return 0
        except Exception as e:
            print(f"❌ Process profiling failed: {e}")
            return 1

    def _check_output_files(self):
        """Check and report on generated profiling files."""
        print("\n📁 Profiling Output Files:")
        print("-" * 30)

        output_files = list(self.output_dir.glob(f"scalene_profile_{self.timestamp}*"))

        if not output_files:
            print("⚠️  No output files found")
            return

        for file_path in output_files:
            size = file_path.stat().st_size
            print(f"✅ {file_path.name} ({size} bytes)")

        # Look for HTML report
        html_file = self.output_dir / f"scalene_profile_{self.timestamp}.html"
        if html_file.exists():
            print(f"\n🌐 Open HTML report: file://{html_file.absolute()}")

    def list_processes(self, filter_name: Optional[str] = None):
        """List running processes that can be profiled."""
        try:
            import psutil

            print("🔍 Running Processes:")
            print("-" * 50)

            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    name = proc.info['name']
                    if filter_name and filter_name.lower() not in name.lower():
                        continue

                    print(f"{proc.info['pid']:>6} {name:<20} {proc.info['cpu_percent']:>5.1f}% {proc.info['memory_percent']:>5.1f}%")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

        except ImportError:
            print("⚠️  psutil not available. Install with: pip install psutil")
        except Exception as e:
            print(f"❌ Error listing processes: {e}")


def main():
    """Main entry point for the profiling script."""
    parser = argparse.ArgumentParser(
        description="Scalene profiling script for PyIceberg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Profiling target options
    parser.add_argument(
        "command",
        nargs="*",
        help="Command to profile (e.g., 'python my_script.py')"
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        default=".bench_out/scalene",
        help="Output directory for profiling results"
    )

    # Profiling mode options
    parser.add_argument(
        "--cpu-only",
        action="store_true",
        help="Profile only CPU usage"
    )

    parser.add_argument(
        "--memory-leak",
        action="store_true",
        help="Enable memory leak detection"
    )

    # Sampling options
    parser.add_argument(
        "--cpu-sampling-rate",
        type=float,
        default=0.01,
        help="CPU sampling rate (default: 0.01)"
    )

    parser.add_argument(
        "--memory-sampling-rate",
        type=float,
        default=0.01,
        help="Memory sampling rate (default: 0.01)"
    )

    # Process identification options
    parser.add_argument(
        "--pid",
        type=int,
        help="Profile specific process by PID"
    )

    parser.add_argument(
        "--find-process",
        help="Find and profile process by name"
    )

    parser.add_argument(
        "--modules",
        nargs="+",
        help="Profile only specific modules (e.g., --modules pyiceberg.io pyiceberg.table)"
    )

    # Utility options
    parser.add_argument(
        "--list-processes",
        action="store_true",
        help="List running processes that can be profiled"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Profiling duration in seconds for process profiling"
    )

    args = parser.parse_args()

    # Initialize profiler
    profiler = ScaleneProfiler(args.output_dir)

    # Handle different profiling modes
    if args.list_processes:
        profiler.list_processes()
        return 0

    if args.pid:
        return profiler.profile_process(args.pid, args.duration)

    if args.find_process:
        pid = profiler._find_process_by_name(args.find_process)
        if pid:
            print(f"🎯 Found process '{args.find_process}' with PID: {pid}")
            return profiler.profile_process(pid, args.duration)
        else:
            print(f"❌ Process '{args.find_process}' not found")
            return 1

    if not args.command:
        parser.print_help()
        return 1

    # Profile the specified command
    return profiler.profile_command(args.command, args)


if __name__ == "__main__":
    sys.exit(main())
