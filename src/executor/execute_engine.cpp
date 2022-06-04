#include "executor/execute_engine.h"
#include <algorithm>
#include <iomanip>
#include "glog/logging.h"
#include "utils/get_files.h"

#define ENABLE_EXECUTE_DEBUG

string path = "./db/";

ExecuteEngine::ExecuteEngine() {
  cout << " __  __ _       _  _____  ____  _" << endl;
  cout << "|  \\/  (_)     (_)/ ____|/ __ \\| |" << endl;
  cout << "| \\  / |_ _ __  _| (___ | |  | | |" << endl;
  cout << "| |\\/| | | '_ \\| |\\___ \\| |  | | |" << endl;
  cout << "| |  | | | | | | |____) | |__| | |____" << endl;
  cout << "|_|  |_|_|_| |_|_|_____/ \\___\\_\\______|" << endl;
  cout << endl;
  vector<string> db_files;
  if (!IsFileExist(path.c_str())) {
    CreateDirectory(path.c_str());
  }
  GetFiles(path, db_files);
  if (db_files.size() == 0) {
    cout << "no database to be loaded" << endl;
  } else {
    for (auto db_file : db_files) {
      cout << "loading " << db_file << "... ";
      dbs_.emplace(db_file.substr(0, db_file.size() - 3), new DBStorageEngine(path + db_file, false));
      cout << "success!" << endl;
    }
  }
  cout << endl;
  // DBStorageEngine(path+"test.db"); //可以创建一个合法的test.db，要求./db目录存在
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  dbs_.emplace(ast->child_->val_, new DBStorageEngine(path + ast->child_->val_ + ".db"));
  cout << "Create " << ast->child_->val_ << " OK" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    cout << "No such database called " << db_name << endl;
    return DB_FAILED;
  } else {
    string path_to_db_file = path + db_name + ".db";
    remove(path_to_db_file.c_str());
    delete dbs_.find(db_name)->second;
    dbs_.erase(db_name);
    cout << "Drop " << db_name << " OK" << endl;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  int max_length = 8;
  for (auto db : dbs_) {
    if ((int)db.first.length() > max_length) {
      max_length = (int)db.first.length();
    }
  }
  cout << "+-" << setw(max_length) << setfill('-') << "-"
       << "-+" << endl;
  cout << "| " << setw(max_length) << setfill(' ') << left << "Database"
       << " |" << endl;
  cout << "+-" << setw(max_length) << setfill('-') << "-"
       << "-+" << endl;
  for (auto db : dbs_) {
    cout << "| " << setw(max_length) << setfill(' ') << left << db.first << " |" << endl;
  }
  cout << "+-" << setw(max_length) << setfill('-') << "-"
       << "-+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    cout << "No such database called " << db_name << endl;
    return DB_FAILED;
  } else {
    current_db_ = db_name;
    cout << "Use " << db_name << " OK" << endl;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  std::vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables)) return DB_FAILED;
  else{
    for ( auto itr = tables.begin();  itr != tables.end(); itr++) {
      //TODO::外围线框
      cout << (*itr)->GetTableName() <<endl;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
//  dberr_t CreateTable(const std::string &table_name, TableSchema *schema, Transaction *txn, TableInfo *&table_info);
  string table_name = ast->child_->val_;
  std::vector<Column *> columns;
  TableSchema table_schema(columns);
  Transaction *txn{};
  TableInfo *table_info;
  //  xjj TODO:Finish this
  if ( dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, &table_schema, txn, table_info) ) return DB_FAILED;
  else {

  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  string table_name = ast->child_->val_;
  if (dbs_[current_db_]->catalog_mgr_->DropTable(table_name)) return DB_FAILED;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  ast = ast->child_;
  string table_name = ast->next_->val_;
  ast = ast->next_;
  ASSERT(ast->next_->type_ == kNodeColumnList, "EXECUTE ERROR: Index keys not found");
  ast = ast->next_;
  auto first_index_key_node = ast->child_;
  auto index_key_node = first_index_key_node;
  vector<string> index_keys;
  while (index_key_node) {
    index_keys.emplace_back(index_key_node->val_);
    index_key_node = index_key_node->next_;
  }
  if (ast->next_) {
    ASSERT(ast->next_->type_ == kNodeIndexType, "EXECUTE ERROR: Unexpected syntax node");
    ast = ast->next_;
    string index_type = ast->child_->val_;
    transform(index_type.begin(), index_type.end(), index_type.begin(), ::tolower);
    if (index_type == "bptree") {
      // TODO: 这里目前并没有index的type入参，默认只有bplustree
      IndexInfo* tmp_index_info;
       dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, tmp_index_info);
      cout << "Create bptree index " << index_name << " OK" << endl;
    } else if (index_type == "hash") {
      // TODO: create hash index
      cout << "Not supported yet" << endl;
      // cout << "Create hash index " << index_name << " OK" << endl;
    } else {
      cout << "Please choose (bptree/hash) index" << endl;
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  // dbs_[current_db_]->catalog_mgr_->DropIndex(index_name); // 我不理解，为什么还要table_name
  cout << "Drop index " << index_name << " OK" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  auto columns_node = ast->child_;
  // 可能是allColumns也可能是一个columnsList
  [[maybe_unused]]bool use_all_columns = true;
  vector<string> columns;
  if (columns_node->type_ == kNodeColumnList) {
    use_all_columns = false;
    auto column = columns_node->child_;
    while (column) {
      columns.emplace_back(column->val_);
      column = column->next_;
    }
  }
  ast = ast->next_;
  string table_name = ast->val_;
  vector<vector<pSyntaxNode>> conditions;
  vector<pSyntaxNode> now_condition;
  // 如果有查询条件
  if (ast->next_) {
    ast = ast->next_->child_;
    while (ast->type_ == kNodeConnector) {
      now_condition.emplace_back(ast->next_);
      string connector = ast->val_;
      if (connector == "or") {
        conditions.emplace_back(now_condition);
        now_condition.clear();
      }
    }
    now_condition.emplace_back(ast);
    conditions.emplace_back(now_condition);
    now_condition.clear();
  }
  // TODO: 对table_name，conditions中的列名，columns中的列名做合法性判断

  // 利用conditions进行查询
  vector<RowId> result;
  if (conditions.size() == 0) {
    TableInfo* tmp_table_info;
    dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tmp_table_info);
    auto table_iter = tmp_table_info->GetTableHeap()->Begin(nullptr);
    while (table_iter != tmp_table_info->GetTableHeap()->End()) {
      result.emplace_back((*table_iter).GetRowId());
    }
  } else {
    // 是否使用索引
    bool use_index = true;
    IndexInfo* chosed_index = nullptr;
    // 是否使用复合索引
    bool use_multi_index = true;
    bool is_all_equal = true;
    // 如果有or，那么不用索引，全表扫描
    if (conditions.size() > 1) {
      use_index = false;
    } else {
      // 此时只有一个全为and连接的条件集合，位于conditions[0]
      for(auto condition : conditions[0]) {
        string condition_operator = condition->val_;
        if (condition_operator != "=") {
          is_all_equal = false;
        }
        if (condition_operator == "<>" || condition_operator == "is" || condition_operator == "not") {
          use_index = false;
          break;
        }
      }
      vector<IndexInfo *> indexes;
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
      // 简单起见，这里只做（所有索引字段条件为等于）的复合索引查询或简单单列索引的索引查询）
      // 先判断有没有必要做复合索引查询
      if (is_all_equal && conditions[0].size() > 1) {
        // 如果可以做复合索引查询，那么找找有没有完全匹配的复合索引
        for (auto index : indexes) {
          auto tmp_columns = index->GetIndexKeySchema()->GetColumns();
          bool all_same = true;
          if (tmp_columns.size() != conditions[0].size()) {
            all_same = false;
          } else {
            for (auto condition : conditions[0]) {
              bool single_same = false;
              for (auto tmp_column : tmp_columns) {
                if (tmp_column->GetName() == condition->child_->val_) {
                  single_same = true;
                  break;
                }
              }
              if (!single_same) {
                all_same = false;
                break;
              }
            }
          }
          if (all_same) {
            chosed_index = index;
            break;
          }
        }
        if (chosed_index == nullptr) {
          use_multi_index = false;
        }
      }
      if (!use_multi_index) {
        // 寻找单个索引
        for (auto index : indexes) {
          auto tmp_columns = index->GetIndexKeySchema()->GetColumns();
          if (tmp_columns.size() > 1) {
            continue;
          } else {
            for (auto condition : conditions[0]) {
              // 找到第一个符合的就行，不过多计算优化
              if (condition->child_->val_ == tmp_columns[0]->GetName()) {
                chosed_index = index;
                break;
              }
            }
          }
        }
      }
    }
    if (use_index && chosed_index != nullptr) {
      // 单个condition
      // 用索引进行查找
      // int key_length = chosed_index->GetKeyLength();
      // 先构建一个索引查找的Row对象
      vector<Field> fields;
      for (auto column : chosed_index->GetIndexKeySchema()->GetColumns()) {
        for (auto condition : conditions[0]) {
          if (condition->child_->val_ == column->GetName()) {
            if (column->GetType() == TypeId::kTypeInt) {
              fields.emplace_back(Field(column->GetType(), stoi(condition->child_->next_->val_)));
            } else if (column->GetType() == TypeId::kTypeChar) {
              string tmp_str = condition->child_->next_->val_;
              fields.emplace_back(Field(column->GetType(), condition->child_->next_->val_, tmp_str.size(), true));
            } else if (column->GetType() == TypeId::kTypeFloat) {
              fields.emplace_back(Field(column->GetType(), (float)atof(condition->child_->next_->val_)));
            }
          }
        }
      }
      Row row(fields);
      if (use_multi_index) {
        if (chosed_index->GetIndex()->ScanKey(row, result, nullptr, "=") == DB_KEY_NOT_FOUND) {
          cout << "empty set" << endl;
          return DB_SUCCESS;
        }
      } else {
        // 用单个列的索引
        for (auto condition : conditions[0]) {
          if (condition->child_->val_ == chosed_index->GetIndexKeySchema()->GetColumn(0)->GetName()) {
            if (chosed_index->GetIndex()->ScanKey(row, result, nullptr, condition->val_) == DB_KEY_NOT_FOUND) {
              cout << "empty set" << endl;
              return DB_SUCCESS;
            } else {
              // TODO：因为单个列的索引查找我是允许有复合条件的，所以还把result过滤一遍

            }
          }
        }
      }
    } else {
      // 可能有多个condition（指or相连的）
      // TODO: 全表扫描，符合的加进result
      TableInfo* tmp_table_info;
      dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tmp_table_info);
      auto tmp_table_iter = tmp_table_info->GetTableHeap()->Begin(nullptr);
      while (tmp_table_iter != tmp_table_info->GetTableHeap()->End()) {
        bool is_satisfied = true;
        // TODO: 遍历条件

        if (is_satisfied) {
          result.emplace_back((*tmp_table_iter).GetRowId());
        }
      }
    }
  }
  if (result.size() == 0) {
    cout << "empty set" << endl;
    return DB_SUCCESS;
  } else {
    // TODO: 打印result（记得根据columns投影）
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
