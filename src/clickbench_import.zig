pub const status_imported = "imported";
pub const status_experimental_fixed_only = "experimental-fixed-only";

pub const profile_hot_minimal = "clickbench-hot-minimal";
pub const profile_hot_native_domain_stats = "clickbench-hot-native-domain-stats";
pub const profile_hot_native_domain_stats_q38 = "clickbench-hot-native-domain-stats-q38";
pub const profile_hot_fixed_only = "clickbench-hot-fixed-only";

pub const decoder_duckdb_vector = "duckdb-c-api-vector";
pub const decoder_duckdb_vector_fixed = "duckdb-c-api-vector-fixed";
pub const decoder_native_parquet_fixed_byte_array = "native-parquet-fixed-byte-array";

pub fn nativeProfile(write_tiny_caches: bool) []const u8 {
    return if (write_tiny_caches) profile_hot_native_domain_stats_q38 else profile_hot_native_domain_stats;
}
