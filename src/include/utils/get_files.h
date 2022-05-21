//
// Created by beet on 2022/5/20.
//

#ifndef MINISQL_GET_FILES_H
#define MINISQL_GET_FILES_H

#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>

using namespace std;
int GetFiles(const string path, vector<string> &files) {
  int iFileCnt = 0;
  DIR *dirptr = NULL;
  struct dirent *dirp;

  if ((dirptr = opendir(path.c_str())) == NULL)  // 打开一个目录
  {
    return 0;
  }
  while ((dirp = readdir(dirptr)) != NULL) {
    if ((dirp->d_type == DT_REG) && 0 == (strcmp(strchr(dirp->d_name, '.'), ".db")))  // 判断是否为文件以及文件后缀名
    {
      files.push_back(dirp->d_name);
    }
    iFileCnt++;
  }
  closedir(dirptr);
  return iFileCnt;
}

int IsFileExist(const char *path) { return !access(path, F_OK); }

int CreateDirectory(const char *path) { return mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH); }

#endif  // MINISQL_GET_FILES_H
