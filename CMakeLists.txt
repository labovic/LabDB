cmake_minimum_required(VERSION 3.22)
project(LabDB C)

set(CMAKE_C_STANDARD 11)

add_executable(LabDB main.c pager.c pager.h table.c table.h database.c database.h utils.c utils.h query.c query.h)
