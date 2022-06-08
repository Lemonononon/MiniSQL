#include "executor/execute_engine.h"
#include <algorithm>
#include <cstring>
#include <iomanip>
#include "glog/logging.h"
#include "utils/get_files.h"

#define ENABLE_EXECUTE_DEBUG

bool IsSatisfiedRow(Row *row, SyntaxNode *condition, uint32_t column_index, TypeId column_type);
string GetFieldString(Field *field, TypeId type);
vector<RowId> GetSatisfiedRowIds(vector<vector<SyntaxNode *>> conditions, TableInfo *table_info,
                                 vector<IndexInfo *> indexes);
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
  if (dbs_.size() == 0) {
    cout << "empty set" << endl;
    return DB_SUCCESS;
  }
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
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables))
    return DB_FAILED;
  else {
    if (tables.size() == 0) {
      cout << "empty set" << endl;
    } else {
      unsigned long max_length = 5;
      for (auto table : tables) {
        if (table->GetTableName().size() > max_length) {
          max_length = table->GetTableName().size();
        }
      }
      cout << "+-" << setw(max_length) << setfill('-') << "-"
           << "-+" << endl;
      cout << "| " << setw(max_length) << setfill(' ') << left << "Table"
           << " |" << endl;
      cout << "+-" << setw(max_length) << setfill('-') << "-"
           << "-+" << endl;
      for (auto table : tables) {
        cout << "| " << setw(max_length) << setfill(' ') << left << table->GetTableName() << " |" << endl;
      }
      cout << "+-" << setw(max_length) << setfill('-') << "-"
           << "-+" << endl;
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  //  dberr_t CreateTable(const std::string &table_name, TableSchema *schema, Transaction *txn, TableInfo *&table_info);
  string table_name = ast->child_->val_;
  std::vector<Column *> columns;
  // 根据语法树建立所有的column
  auto node = ast->child_->next_->child_;
  uint32_t column_index = 0;

  //记录哪些列需要建立unique index
  vector<vector<string>> unique_indexes;
  // 记录所有的column对象，后续需要所有column对象的指针来创建TableSchema
  // vector<Column> columns_list;
  while (node && node->type_ != kNodeColumnList) {
    // column默认是可以为空，不unique的
    bool flag_nullable = true;
    bool flag_unique = false;
    // 约束条件

    string constraint;
    if (node->val_) {
      constraint = node->val_;
      // 如果是unique，只需要将Column中的unique_置为true
      if (constraint == "unique") {
        flag_unique = true;
        vector<string> v;
        v.emplace_back(string(node->child_->val_));
        unique_indexes.emplace_back(v);
      }
      // 暂不支持not null
    }
    string column_name = node->child_->val_;
    string column_type = node->child_->next_->val_;
    // uint32_t length;
    //    Column column_list = new Column*[];
    // int, float, char
    if (column_type == "char") {
      uint32_t column_length = atoi(node->child_->next_->child_->val_);
      // Column column(column_name, kTypeChar, column_length, flag_nullable, flag_unique);
      auto column = new Column(column_name, kTypeChar, column_length, column_index, flag_nullable, flag_unique);
      columns.emplace_back(column);
    } else if (column_type == "int") {
      // Column column(column_name, kTypeInt, column_index++, flag_nullable, flag_unique);
      auto column = new Column(column_name, kTypeInt, column_index++, flag_nullable, flag_unique);
      columns.emplace_back(column);
    } else if (column_type == "float") {
      // Column column(column_name, kTypeFloat, column_index++, flag_nullable, flag_unique);
      auto column = new Column(column_name, kTypeFloat, column_index++, flag_nullable, flag_unique);
      columns.emplace_back(column);
    }
    node = node->next_;
  }

  //  TableSchema table_schema(columns);
  auto table_schema = new TableSchema(columns);
  //  Transaction *txn{};
  TableInfo *table_info;
  // 建表
  if (dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, table_schema, nullptr, table_info) ==
      DB_TABLE_ALREADY_EXIST) {
    cout << "ERROR: Table name already exist" << endl;
    return DB_FAILED;
  }
  // 处理末尾的primary key, 为主键建立索引
  vector<string> primary_keys;
  if (node) {
    auto primary_key_node = node->child_;
    while (primary_key_node) {
      primary_keys.emplace_back(primary_key_node->val_);
      primary_key_node = primary_key_node->next_;
    }
  }
  string primary_index_name = "PRIMARY";
  IndexInfo *primary_index_info;
  if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, primary_index_name, primary_keys, nullptr, primary_index_info)){
//    cout << "Primary Key Index already exist" << endl;
    return DB_FAILED;
  }

  //为所有的unique列建立索引
  for ( auto unique_itr = unique_indexes.begin(); unique_itr != unique_indexes.end() ; unique_itr++) {
    string unique_index_name = (*unique_itr)[0];
    IndexInfo *unique_index_info;
    if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, unique_index_name, *unique_itr, nullptr, unique_index_info)){
      cout << "Create unique index on " << unique_index_name << "failed" << endl;
      return DB_FAILED;
    }
  }

  dbs_[current_db_]->bpm_->FlushAllPages();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  switch (dbs_[current_db_]->catalog_mgr_->DropTable(table_name)) {
    case DB_FAILED:
      cout << "ERROR: Table still used" << endl;
      return DB_FAILED;
    case DB_TABLE_NOT_EXIST:
      cout << "ERROR: Table not exist" << endl;
      return DB_FAILED;
    default:
      return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }

  //获取所有的表
  std::vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables))
    return DB_FAILED;

  //没有表，结束
  if (tables.empty()){
    cout << "empty set" << endl;
    return DB_SUCCESS;
  }

  std::vector<IndexInfo *> indexes;
  std::vector<IndexInfo *> tmp_indexes;
  for ( auto itr = tables.begin(); itr != tables.end(); itr++) {
    if ( dbs_[current_db_]->catalog_mgr_->GetTableIndexes(current_db_, tmp_indexes) != DB_SUCCESS) return DB_FAILED;
    //insert效率会低? 不过一般index较少，影响不大
    indexes.insert(indexes.end(), tmp_indexes.begin(), tmp_indexes.end());
  }

  if (indexes.empty()) {
    cout << "empty set" <<endl;
    return DB_SUCCESS;
  }else {
    int32_t max_length = 5;
    for (auto index : indexes) {
      if (index->GetKeyLength() > max_length)
        max_length = index->GetKeyLength();
      }
    cout << "+-" << setw(max_length) << setfill('-') << "-"
         << "-+" << endl;
    cout << "| " << setw(max_length) << setfill(' ') << left << "Index"
         << " |" << endl;
    cout << "+-" << setw(max_length) << setfill('-') << "-"
         << "-+" << endl;
    for (auto index : indexes) {
      cout << "| " << setw(max_length) << setfill(' ') << left << index->GetIndexName() << " |" << endl;
    }
    cout << "+-" << setw(max_length) << setfill('-') << "-"
         << "-+" << endl;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  // TODO: 只有unique的可以创建index
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  ast = ast->child_;
  string table_name = ast->next_->val_;
  ast = ast->next_;
  ASSERT(ast->next_->type_ == kNodeColumnList, "ERROR: Index keys not found");
  ast = ast->next_;
  auto first_index_key_node = ast->child_;
  auto index_key_node = first_index_key_node;
  vector<string> index_keys;
  while (index_key_node) {
    index_keys.emplace_back(index_key_node->val_);
    index_key_node = index_key_node->next_;
  }
  // 记录是否存在unique键，只有索引列中有一个是unique的，才可以建立索引
  bool is_unique = false;
  TableInfo* table_info;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "ERROR: Table not exist" << endl;
    return DB_FAILED;
  }
  for (auto index_key : index_keys) {
    uint32_t tmp_column_index;
    if (table_info->GetSchema()->GetColumnIndex(index_key, tmp_column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      cout << "ERROR: Column not exist" << endl;
      return DB_FAILED;
    }
    if (table_info->GetSchema()->GetColumn(tmp_column_index)->IsUnique()) {
      is_unique = true;
      break;
    }
  }
  if (!is_unique) {
    cout << "ERROR: Only when contains a unique column can you build index" << endl;
    return DB_FAILED;
  }
  if (ast->next_) {
    ASSERT(ast->next_->type_ == kNodeIndexType, "ERROR: Unexpected syntax node");
    ast = ast->next_;
    string index_type = ast->child_->val_;
    transform(index_type.begin(), index_type.end(), index_type.begin(), ::tolower);
    if (index_type != "bptree") {
      if (index_type == "hash") {
        cout << "Not supported yet" << endl;
        return DB_FAILED;
      } else {
        cout << "Please choose (bptree/hash) index" << endl;
        return DB_FAILED;
      }
    }
  }
  IndexInfo *tmp_index_info;
  dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, tmp_index_info);
  TableInfo *tmp_table_info;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tmp_table_info);
  uint32_t *column_index = new uint32_t[index_keys.size()];
  for (unsigned long i = 0; i < index_keys.size(); ++i) {
    tmp_table_info->GetSchema()->GetColumnIndex(index_keys[i], column_index[i]);
  }
  auto table_iter = tmp_table_info->GetTableHeap()->Begin(nullptr);
  for (; table_iter != tmp_table_info->GetTableHeap()->End(); ++table_iter) {
    // 遍历整个表，对每一行都取出响应Index keys插入索引
    vector<Field> fields;
    for (unsigned long i = 0; i < index_keys.size(); ++i) {
      fields.emplace_back(*(table_iter->GetField(column_index[i])));
    }
    Row tmp_row(fields);
    tmp_index_info->GetIndex()->InsertEntry(tmp_row, table_iter->GetRowId(), nullptr);
  }
  delete[] column_index;
  cout << "Create bptree index " << index_name << " OK" << endl;
  dbs_[current_db_]->bpm_->FlushAllPages();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  // dbs_[current_db_]->catalog_mgr_->DropIndex(index_name); // 我不理解，为什么还要table_name
  cout << "Drop index " << index_name << " OK" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  auto columns_node = ast->child_;
  ast = ast->child_;
  // 可能是allColumns也可能是一个columnsList
  bool use_all_columns = true;
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
      now_condition.emplace_back(ast->child_->next_);
      string connector = ast->val_;
      if (connector == "or") {
        conditions.emplace_back(now_condition);
        now_condition.clear();
      }
      ast = ast->child_;
    }
    now_condition.emplace_back(ast);
    conditions.emplace_back(now_condition);
    now_condition.clear();
  }
  // 利用conditions进行查询
  TableInfo *table_info;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "ERROR: Table not exist" << endl;
    return DB_FAILED;
  }
  vector<uint32_t> column_indexes;
  vector<TypeId> column_types;
  // 如果是select * ，那就是所有columns
  if (use_all_columns) {
    //    cout << table_info->GetSchema()->GetColumns().size() << endl;
    for (auto column : table_info->GetSchema()->GetColumns()) {
      columns.emplace_back(column->GetName());
      //      cout << column->GetName() << endl;
    }
  }
  // 需要知道要取一个row中哪些field的值，（投影操作），所以需要先得到这些field的index，也就是column_indexes
  for (auto column : columns) {
    uint32_t column_index;
    if (table_info->GetSchema()->GetColumnIndex(column, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      cout << "ERROR: Column not exist" << endl;
      return DB_FAILED;
    }
    column_types.emplace_back(table_info->GetSchema()->GetColumn(column_index)->GetType());
    column_indexes.emplace_back(column_index);
  }
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
  cout << "We have " << indexes.size() << " indexes" << endl;
  auto result = GetSatisfiedRowIds(conditions, table_info, indexes);
  if (result.size() == 0) {
    cout << "empty set" << endl;
    return DB_SUCCESS;
  } else {
    // 打印result
    vector<Row> result_rows;
    // 需要知道要取一个row中哪些field的值，（投影操作），所以需要先得到这些field的index，也就是column_indexes
    for (unsigned long i = 0; i < result.size(); i++) {
      auto tmp_row = Row(result[i]);
      table_info->GetTableHeap()->GetTuple(&tmp_row, nullptr);
      result_rows.emplace_back(tmp_row);
    }
    for (auto column : columns) {
      uint32_t column_index;
      table_info->GetSchema()->GetColumnIndex(column, column_index);
      column_types.emplace_back(table_info->GetSchema()->GetColumn(column_index)->GetType());
      column_indexes.emplace_back(column_index);
    }
    unsigned long *print_length = new unsigned long[columns.size()];
    for (unsigned long i = 0; i < columns.size(); ++i) {
      print_length[i] = columns[i].size();
    }
    // 先统计一下每一列的长度
    for (auto result_row : result_rows) {
      for (unsigned long i = 0; i < column_indexes.size(); i++) {
        // 如果长度长于print_length[i]，那么更新print_length
        unsigned long tmp_length = GetFieldString(result_row.GetField(column_indexes[i]), column_types[i]).size();
        if (tmp_length > print_length[i]) {
          print_length[i] = tmp_length;
        }
      }
    }
    // 打印列名行
    for (unsigned long i = 0; i < columns.size(); ++i) {
      if (i == 0) {
        cout << "+-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      } else {
        cout << "-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      }
    }
    cout << endl;
    for (unsigned long i = 0; i < columns.size(); ++i) {
      if (i == 0) {
        cout << "| " << setw(print_length[i]) << setfill(' ') << left << columns[i] << " |";
      } else {
        cout << " " << setw(print_length[i]) << setfill(' ') << left << columns[i] << " |";
      }
    }
    cout << endl;
    for (unsigned long i = 0; i < columns.size(); ++i) {
      if (i == 0) {
        cout << "+-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      } else {
        cout << "-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      }
    }
    cout << endl;
    // 打印数据
    for (auto result_row : result_rows) {
      for (unsigned long i = 0; i < columns.size(); ++i) {
        if (i == 0) {
          cout << "| " << setw(print_length[i]) << setfill(' ') << left
               << GetFieldString(result_row.GetField(column_indexes[i]), column_types[i]) << " |";
        } else {
          cout << " " << setw(print_length[i]) << setfill(' ') << left
               << GetFieldString(result_row.GetField(column_indexes[i]), column_types[i]) << " |";
        }
      }
      cout << endl;
    }
    for (unsigned long i = 0; i < columns.size(); ++i) {
      if (i == 0) {
        cout << "+-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      } else {
        cout << "-" << setw(print_length[i]) << setfill('-') << "-"
             << "-+";
      }
    }
    cout << endl;
    delete[] print_length;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No current database" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *table_info;
  dberr_t get_table_result;
  get_table_result = dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  // 获取TableInfo失败，返回状态
  if (get_table_result != DB_SUCCESS) return get_table_result;
  //  cout << table_info->GetTableName() << endl;
  vector<Field> fields;
  auto columns = table_info->GetSchema()->GetColumns();
  //  for (auto column : columns) {
  //    cout << column->GetName() << endl;
  //  }
  // 第一个value
  auto val_node = ast->child_->next_->child_;
  // 遍历以获取每个field的类型，如果语法树value的结点不够，则报错
  for (auto itr = columns.begin(); itr != columns.end(); itr++) {
    // node为空，则insert的数据有错
    if (!val_node) {
      cout << "wrong insert format!" << endl;
      return DB_FAILED;
    }
    uint32_t type = (*itr)->GetType();
    //    cout << "column type: " << type << endl;
    //    cout << val_node->val_ << endl;
    // int
    if (type == kTypeInt) {
      fields.emplace_back(Field(kTypeInt, atoi(val_node->val_)));
    }
    // float
    else if (type == kTypeFloat) {
      fields.emplace_back(Field(kTypeFloat, static_cast<float>(atof(val_node->val_))));
    }
    // char
    else {
      fields.emplace_back(Field(kTypeChar, val_node->val_, strlen(val_node->val_), true));
    }
    val_node = val_node->next_;
  }
  //  int field_type = table_info->GetSchema()->GetColumn()->GetType()

  Row row(fields);
  // 插入数据
  table_info->GetTableHeap()->InsertTuple(row, nullptr);
  dbs_[current_db_]->bpm_->FlushAllPages();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  vector<vector<pSyntaxNode>> conditions;
  vector<pSyntaxNode> now_condition;
  // 首先根据语法树，找到需要从哪张表里删除
  ast = ast->child_;
  string table_name = ast->val_;
  // 如果没有条件，则删除所有table中的记录
  TableInfo *table_info;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    cout << "ERROR: Table not exist" << endl;
    return DB_FAILED;
  }
  TableHeap *table_heap = table_info->GetTableHeap();
  if (ast->next_) {
    ast = ast->next_->child_;
    while (ast->type_ == kNodeConnector) {
      now_condition.emplace_back(ast->child_->next_);
      string connector = ast->val_;
      if (connector == "or") {
        conditions.emplace_back(now_condition);
        now_condition.clear();
      }
      ast = ast->child_;
    }
    now_condition.emplace_back(ast);
    conditions.emplace_back(now_condition);
    now_condition.clear();
  }
  // 利用conditions进行查询
  vector<IndexInfo *> indexes;
  vector<RowId> results = GetSatisfiedRowIds(conditions, table_info, indexes);
  for (uint32_t i = 0; i < results.size(); i++) {
    table_heap->ApplyDelete(results[i], nullptr);
    dbs_[current_db_]->bpm_->FlushAllPages();
  }
  cout << "Query OK, " << results.size() << " rows affected" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  // TODO: xjj, finish this

  return DB_SUCCESS;
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

// 判断Row对象是否满足condition，condition中涉及到的column在row中为第column_index个
bool IsSatisfiedRow(Row *row, SyntaxNode *condition, uint32_t column_index, TypeId column_type) {
  bool is_satisfied = true;
  string condition_operator = condition->val_;
  if (condition_operator == "=") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == "<") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareLessThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareLessThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareLessThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == "<=") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareLessThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareLessThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareLessThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == ">") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareGreaterThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareGreaterThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareGreaterThan(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == ">=") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareGreaterThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareGreaterThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareGreaterThanEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == "<>") {
    if (column_type == TypeId::kTypeInt) {
      auto tmp_field = Field(column_type, stoi(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareNotEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeChar) {
      string tmp_str = condition->child_->next_->val_;
      auto tmp_field = Field(column_type, condition->child_->next_->val_, tmp_str.size(), true);
      if (row->GetField(column_index)->CompareNotEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    } else if (column_type == TypeId::kTypeFloat) {
      auto tmp_field = Field(column_type, (float)atof(condition->child_->next_->val_));
      if (row->GetField(column_index)->CompareNotEquals(tmp_field) == CmpBool::kFalse) {
        is_satisfied = false;
      }
    }
  } else if (condition_operator == "is") {
    if (!row->GetField(column_index)->IsNull()) {
      is_satisfied = false;
    }
  } else if (condition_operator == "not") {
    if (row->GetField(column_index)->IsNull()) {
      is_satisfied = false;
    }
  }
  return is_satisfied;
}

string GetFieldString(Field *field, TypeId type) {
  if (type == TypeId::kTypeInt) {
    return to_string(field->GetInt());
  } else if (type == TypeId::kTypeFloat) {
    return to_string(field->GetFloat());
  } else if (type == TypeId::kTypeChar) {
    string tmp_string = field->GetChars();
    return tmp_string;
  } else {
    throw Exception("What is this?");
  }
}

vector<RowId> GetSatisfiedRowIds(vector<vector<SyntaxNode *>> conditions, TableInfo *table_info,
                                 vector<IndexInfo *> indexes) {
  vector<RowId> result;
  if (conditions.size() == 0) {
    auto table_iter = table_info->GetTableHeap()->Begin(nullptr);
    while (table_iter != table_info->GetTableHeap()->End()) {
      result.emplace_back((*table_iter).GetRowId());
      ++table_iter;
    }
  } else {
    // 是否使用索引
    bool use_index = true;
    IndexInfo *chosed_index = nullptr;
    // 是否使用复合索引
    bool use_multi_index = true;
    bool is_all_equal = true;
    // 如果有or，那么不用索引，全表扫描
    if (conditions.size() > 1) {
      use_index = false;
    } else {
      // 此时只有一个全为and连接的条件集合，位于conditions[0]
      for (auto condition : conditions[0]) {
        string condition_operator = condition->val_;
        if (condition_operator != "=") {
          is_all_equal = false;
        }
        if (condition_operator == "<>" || condition_operator == "is" || condition_operator == "not") {
          use_index = false;
          break;
        }
      }
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
      cout << "Use index " << chosed_index->GetIndexName() << endl;
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
          return result;
        }
      } else {
        // 用单个列的索引
        for (auto condition : conditions[0]) {
          if (condition->child_->val_ == chosed_index->GetIndexKeySchema()->GetColumn(0)->GetName()) {
            if (chosed_index->GetIndex()->ScanKey(row, result, nullptr, condition->val_) == DB_KEY_NOT_FOUND) {
              return result;
            } else {
              // 因为单个列的索引查找我是允许有复合条件的，所以还把result过滤一遍
              for (auto item = result.begin(); item != result.end();) {
                Row tmp_row(*item);
                table_info->GetTableHeap()->GetTuple(&tmp_row, nullptr);
                // 这里是遍历conditions[0]的第二层，外面那层是为了找到索引列，里面这层是对索引列条件选出来的结果做其他条件的筛选
                bool is_item_satisfied = true;
                for (auto tmp_condition : conditions[0]) {
                  uint32_t column_index;
                  table_info->GetSchema()->GetColumnIndex(tmp_condition->child_->val_, column_index);
                  auto column_type = table_info->GetSchema()->GetColumn(column_index)->GetType();
                  if (!IsSatisfiedRow(&tmp_row, tmp_condition, column_index, column_type)) {
                    is_item_satisfied = false;
                    break;
                  }
                }
                if (!is_item_satisfied) {
                  result.erase(item);
                } else {
                  item++;
                }
              }
            }
            break;
          }
        }
      }
    } else {
      cout << "Scan through the whole table" << endl;
      // 可能有多个condition（指or相连的）
      // 全表扫描，符合的加进result
      auto tmp_table_iter = table_info->GetTableHeap()->Begin(nullptr);
      while (tmp_table_iter != table_info->GetTableHeap()->End()) {
        bool is_satisfied = false;
        // 遍历条件
        for (auto divided_conditions : conditions) {
          bool is_single_satisfied = true;
          // 这是一坨and连接的条件，只要有一个挂了就整个挂
          for (auto condition : divided_conditions) {
            // 获取这个condition对应的column序号，才能在field里取值
            uint32_t column_index;
            table_info->GetSchema()->GetColumnIndex(condition->child_->val_, column_index);
            auto column_type = table_info->GetSchema()->GetColumn(column_index)->GetType();
            if (!IsSatisfiedRow(tmp_table_iter.operator->(), condition, column_index, column_type)) {
              is_single_satisfied = false;
              break;
            }
          }
          if (is_single_satisfied) {
            // 在一堆or中，只要有一个满足即可
            is_satisfied = true;
            break;
          }
        }
        if (is_satisfied) {
          result.emplace_back((*tmp_table_iter).GetRowId());
        }
        ++tmp_table_iter;
      }
    }
  }
  return result;
}