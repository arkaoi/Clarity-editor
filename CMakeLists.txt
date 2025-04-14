cmake_minimum_required(VERSION 3.12...3.31)
project(service_template CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(DownloadUserver)

find_package(userver COMPONENTS core QUIET)
if(NOT userver_FOUND)
  download_userver(TRY_DIR third_party/userver)
endif()

userver_setup_environment()

add_library(${PROJECT_NAME}_objs OBJECT
        src/db_base.cpp
)
target_link_libraries(${PROJECT_NAME}_objs PUBLIC userver::core)

add_executable(${PROJECT_NAME} src/main.cpp
        src/db_handler.cpp
        src/sstable.cpp
        src/wal.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}_objs)

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


enable_testing()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/venv/bin/pytest")
  set(PYTEST_EXECUTABLE "${CMAKE_CURRENT_SOURCE_DIR}/venv/bin/pytest" CACHE STRING "Path to pytest in venv" FORCE)
else()
  find_program(PYTEST_EXECUTABLE NAMES pytest)
endif()

if(PYTEST_EXECUTABLE)
  add_test(NAME RunPythonTests
          COMMAND ${PYTEST_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests)
endif()

userver_testsuite_add(
        SERVICE_TARGET ${PROJECT_NAME}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tests
        PYTEST_ARGS
        --service-config=${CMAKE_CURRENT_SOURCE_DIR}/configs/config.yaml
        --service-binary=$<TARGET_FILE:${PROJECT_NAME}>
        --service-config-vars=${CMAKE_CURRENT_SOURCE_DIR}/configs/config.yaml
)