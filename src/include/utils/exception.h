//
// Created by beet on 2022/5/10.
//

#ifndef MINISQL_EXCEPTION_H
#define MINISQL_EXCEPTION_H

#include <iostream>
#include <stdexcept>

class Exception : public std::runtime_error {
public:
  Exception(std::string message) : std::runtime_error(message) {
    std::string exception_message = "ERROR: " + message + "\n";
    std::cerr << exception_message;
  }
};

#endif  // MINISQL_EXCEPTION_H
