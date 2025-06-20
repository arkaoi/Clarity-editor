cmake_minimum_required(VERSION 3.12...3.31)
project(clarity CXX)





set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(DownloadUserver)

find_package(userver COMPONENTS core QUIET)
if(NOT userver_FOUND)
  download_userver(TRY_DIR third_party/userver)
endif()

userver_setup_environment()

include_directories(${CMAKE_SOURCE_DIR}/src)

find_package(Boost REQUIRED COMPONENTS iostreams)
if(NOT Boost_FOUND)
  message(FATAL_ERROR "Boost not found")
endif()

add_library(${PROJECT_NAME}_objs OBJECT
    src/base/db_base.cpp
    src/sstable/sstable.cpp
    src/wal/wal.cpp
    src/bloom/bloom.cpp
)

target_link_libraries(${PROJECT_NAME}_objs PUBLIC userver::core Boost::iostreams)

add_executable(${PROJECT_NAME} src/main.cpp
        src/handlers/db_handler.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}_objs)

add_executable(${PROJECT_NAME}_unittest

)
target_link_libraries(${PROJECT_NAME}_unittest PRIVATE ${PROJECT_NAME}_objs userver::utest)
add_google_tests(${PROJECT_NAME}_unittest)

include(GNUInstallDirs)

if(DEFINED ENV{PREFIX})
  message(STATUS "Set install prefix: $ENV{PREFIX}")
  file(TO_CMAKE_PATH "$ENV{PREFIX}" PREFIX_PATH)
  set(CMAKE_INSTALL_PREFIX "${PREFIX_PATH}")
endif()

file(GLOB CONFIGS_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/configs/*.yaml
        ${CMAKE_CURRENT_SOURCE_DIR}/configs/*.json
)

install(TARGETS ${PROJECT_NAME}
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT ${PROJECT_NAME})

install(FILES ${CONFIGS_FILES}
        DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/${PROJECT_NAME}
        COMPONENT ${PROJECT_NAME})

userver_testsuite_add(
        SERVICE_TARGET ${PROJECT_NAME}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests
        PYTEST_ARGS
        --service-config=${CMAKE_CURRENT_SOURCE_DIR}/configs/config.yaml
        --service-binary=$<TARGET_FILE:${PROJECT_NAME}>
        --service-config-vars=${CMAKE_CURRENT_SOURCE_DIR}/configs/config.yaml
)
