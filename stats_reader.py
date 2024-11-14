import pstats

# Load the profiling results
stats = pstats.Stats('profile_results.prof')
stats.sort_stats('cumulative').print_stats(20)  # Sort by cumulative time and show top 20 functions
stats.sort_stats('time').print_stats(20)  # Sort by internal time and show top 20 functions
