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
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  return DB_FAILED;
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
      // TODO: create bptree index
      // dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, IndexInfo::Create(new SimpleMemHeap()));
      cout << "Create bptree index " << index_name << " OK" << endl;
    } else if (index_type == "hash") {
      // TODO: create hash index
      cout << "Create hash index " << index_name << " OK" << endl;
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
