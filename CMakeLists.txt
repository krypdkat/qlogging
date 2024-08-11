cmake_minimum_required(VERSION 3.1)
project(qlogging CXX)
set (CMAKE_CXX_STANDARD 11)
SET(FILES ${CMAKE_SOURCE_DIR}/connection.cpp
		  ${CMAKE_SOURCE_DIR}/parser.cpp
		${CMAKE_SOURCE_DIR}/keyUtils.cpp
)
ADD_EXECUTABLE(qlogging main.cpp ${FILES})