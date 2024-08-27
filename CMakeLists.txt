cmake_minimum_required(VERSION 3.1)
project(qlogging CXX)
set (CMAKE_CXX_STANDARD 11)
SET(CXX_QLOGGING_FLAGS "-Wunused-variable")
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${CXX_QLOGGING_FLAGS}")
SET(FILES ${CMAKE_SOURCE_DIR}/connection.cpp
		  ${CMAKE_SOURCE_DIR}/parser.cpp
		${CMAKE_SOURCE_DIR}/keyUtils.cpp
)
ADD_EXECUTABLE(qlogging main.cpp ${FILES})
set_property(TARGET qlogging PROPERTY COMPILE_WARNING_AS_ERROR ON)
