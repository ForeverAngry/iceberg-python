
import pstats
import sys

def analyze_profile(profile_path, top_n=20):
    """
    Analyzes a cProfile .prof file and prints the top N functions by cumulative time.
    """
    try:
        stats = pstats.Stats(profile_path)
        print(f"Analyzing profile: {profile_path}")
        print("-" * 80)
        stats.sort_stats(pstats.SortKey.CUMULATIVE).print_stats(top_n)
    except Exception as e:
        print(f"Error analyzing profile {profile_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_profile.py <path_to_prof_file>")
        sys.exit(1)
    
    profile_file = sys.argv[1]
    analyze_profile(profile_file)
