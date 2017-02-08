/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   logger.hpp
 * Author: Pooja Nilangekar
 *
 * Created on June 16, 2016, 12:26 PM
 */

#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <iostream>
#include <thread>
#include <mutex>
#include <ctime>

extern std::clock_t start, current;
std::mutex std_out;

#define ERROR 0
#define INFO 1
#define WARNING 2
#define DEBUG 3

/*
 *  print_msg is a macro which prints the messages to stdout.
 */
#define print_msg(msg,level) { \
    \
    std_out.lock(); \
    \
    switch(level) { \
        case ERROR: \
            std::cout << "ERROR:\t"; \
            break; \
        case INFO: \
            std::cout << "INFO:\t"; \
            break; \
        case WARNING: \
            std::cout << "WARNING:\t"; \
            break; \
        case DEBUG: \
            std::cout << "DEBUG:\t"; \
            break;    \
    } \
    \
    current = std::clock(); \
    std::cout << __func__ << ":" << __LINE__ << "(" << ((current - start)/(double)(CLOCKS_PER_SEC)) << "sec):\t"<<msg<<"\n"; \
    \
    std_out.unlock(); \
    }

#endif /* LOGGER_HPP */

