#include "CPUFeatures.hpp"

// Define static members (must be in exactly one translation unit)
bool CPUFeatures::has_avx2_cached = false;
bool CPUFeatures::has_aesni_cached = false;
bool CPUFeatures::has_pclmulqdq_cached = false;
bool CPUFeatures::has_bmi2_cached = false;
bool CPUFeatures::has_fma_cached = false;
bool CPUFeatures::has_popcnt_cached = false;
bool CPUFeatures::initialized = false;
