cmake_minimum_required(VERSION 3.20)
project(ORAM)
set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall")
set(CMAKE_BUILD_TYPE Debug)


# add_subdirectory(${ORAM_SOURCE_DIR}/src)

aux_source_directory(${ORAM_SOURCE_DIR}/src SRC)
list(REMOVE_ITEM SRC ${ORAM_SOURCE_DIR}/src/csprng.cpp)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
# add_compile_options(-march=native -O3 -Wall -pthread)
add_compile_options(-march=native -O0 -Wall -pthread)
include_directories(/usr/local/Cellar ${Boost_INCLUDE_DIRS} ${ORAM_SOURCE_DIR}/include /usr/local/include)
# include_directories(${ORAM_SOURCE_DIR}/include)
link_directories(${ORAM_SOURCE_DIR}/include)
link_directories(${ORAM_SOURCE_DIR}/lib)
add_executable(main ${SRC})
# target_link_libraries(main zmq) 
target_link_libraries(main 
    PRIVATE 
    zmq 
    emp-tool            # 必须链接 emp-tool 自身
    OpenSSL::SSL        # 解决 EVP_... 报错
    OpenSSL::Crypto     # 解决 EVP_... 报错
    pthread             # 解决线程报错
    ${Boost_LIBRARIES}  # 链接 Boost
)