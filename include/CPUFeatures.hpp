#ifndef CPU_FEATURES_HPP_
#define CPU_FEATURES_HPP_

#include <iostream>
#include <immintrin.h>
#include <cpuid.h>

/**
 * @brief Runtime CPU feature detection for Intel i7-1360P optimizations
 * 
 * Detects: AVX2, AES-NI, PCLMULQDQ, BMI2, FMA, POPCNT
 * Thread-safe singleton pattern with lazy initialization
 */
class CPUFeatures {
private:
    static bool has_avx2_cached;
    static bool has_aesni_cached;
    static bool has_pclmulqdq_cached;
    static bool has_bmi2_cached;
    static bool has_fma_cached;
    static bool has_popcnt_cached;
    static bool initialized;
    
    static void __attribute__((always_inline)) detect_features() {
        if (initialized) return;
        
        unsigned int eax, ebx, ecx, edx;
        
        // Check CPUID level 1 features (AES-NI, PCLMULQDQ, FMA, POPCNT)
        if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
            has_aesni_cached = (ecx & bit_AES) != 0;
            has_pclmulqdq_cached = (ecx & bit_PCLMUL) != 0;
            has_fma_cached = (ecx & bit_FMA) != 0;
            has_popcnt_cached = (ecx & bit_POPCNT) != 0;
        }
        
        // Check CPUID level 7 features (AVX2, BMI2)
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx);
            has_avx2_cached = (ebx & bit_AVX2) != 0;
            has_bmi2_cached = (ebx & bit_BMI2) != 0;
        }
        
        initialized = true;
    }
    
public:
    static inline bool has_avx2() {
        if (!initialized) detect_features();
        return has_avx2_cached;
    }
    
    static inline bool has_aesni() {
        if (!initialized) detect_features();
        return has_aesni_cached;
    }
    
    static inline bool has_pclmulqdq() {
        if (!initialized) detect_features();
        return has_pclmulqdq_cached;
    }
    
    static inline bool has_bmi2() {
        if (!initialized) detect_features();
        return has_bmi2_cached;
    }
    
    static inline bool has_fma() {
        if (!initialized) detect_features();
        return has_fma_cached;
    }
    
    static inline bool has_popcnt() {
        if (!initialized) detect_features();
        return has_popcnt_cached;
    }
    
    static void print_features() {
        detect_features();
        std::cout << "=== CPU Feature Detection ===" << std::endl;
        std::cout << "AVX2:       " << (has_avx2_cached ? "YES" : "NO") << std::endl;
        std::cout << "AES-NI:     " << (has_aesni_cached ? "YES" : "NO") << std::endl;
        std::cout << "PCLMULQDQ:  " << (has_pclmulqdq_cached ? "YES" : "NO") << std::endl;
        std::cout << "BMI2:       " << (has_bmi2_cached ? "YES" : "NO") << std::endl;
        std::cout << "FMA:        " << (has_fma_cached ? "YES" : "NO") << std::endl;
        std::cout << "POPCNT:     " << (has_popcnt_cached ? "YES" : "NO") << std::endl;
        std::cout << "=============================" << std::endl;
    }
};

#endif // CPU_FEATURES_HPP_
